package com.psql.service;

import com.psql.config.AppConfig;
import com.psql.config.AuditConfig;
import com.psql.kafka.KafkaConsumer;
import com.psql.kafka.KafkaProducer;
import org.apache.commons.lang.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.sql.DriverManager.getConnection;

@Service
public class PsqlParserService {

    private static final Logger log = LoggerFactory.getLogger(PsqlParserService.class);
    @Autowired
    private AppConfig appConfig;
    @Autowired
    private AuditConfig auditConfig;
    @Autowired
    KafkaProducer kafkaProducer;
    @Autowired
    KafkaConsumer kafkaConsumer;
    @Value(value = "${log.topic.name}")
    private String topic;

    private String createTableQuery = "";

    private enum DataType {bigint, name, date}
    private enum Operation {insert, update, delete}

    /**
     * Parse data obtained from WAL
     *
     * @throws SQLException
     * @throws InterruptedException
     */

    public void parseData() throws SQLException, InterruptedException {
        Properties props = getProperties();
        try (Connection replicationConnection = getConnection(auditConfig.getUrl(), props)) {
            try {
                    PGConnection replConnection = replicationConnection.unwrap(PGConnection.class);
                    //create replication slot
                    replConnection.getReplicationAPI()
                            .createReplicationSlot()
                            .logical()
                            .withSlotName("logging")
                            .withOutputPlugin("wal2json")
                            .make();

                    // get replication stream
                    PGReplicationStream stream =
                            replConnection.getReplicationAPI()
                                    .replicationStream()
                                    .logical()
                                    .withSlotName("logging")
                                    .withSlotOption("include-xids", false)
                                    .withStatusInterval(20, TimeUnit.SECONDS)
                                    .start();

                    while (true) {
                        //non blocking receive message
                        ByteBuffer msg = stream.readPending();
                        if (msg == null) {
                            TimeUnit.MILLISECONDS.sleep(10L);
                            continue;
                        }

                        int offset = msg.arrayOffset();
                        byte[] source = msg.array();
                        int length = source.length - offset;

                        String data = new String(source, offset, length);
                        if (StringUtils.isNotEmpty(data)) {
                            kafkaProducer.postData(topic, data);
                        }
                        //feedback
                        stream.setAppliedLSN(stream.getLastReceiveLSN());
                        stream.setFlushedLSN(stream.getLastReceiveLSN());

                    }
                }
                finally {
                    log.info("Process ........");
                }
            }
        }


    /**
     * Add log for ddl operations.
     *
     * @param data              data obtained from WAL

     * @throws SQLException
     */
    public void addLogForData(String data) throws SQLException {
        JSONObject dataObject = new JSONObject(data);
        JSONArray dataArray = dataObject.getJSONArray("change");
        Properties props = getProperties();
        Connection logConnection = getConnection(auditConfig.getUrl(), props);
        Statement preparedStatement = logConnection.createStatement();
        int dataLength = dataArray.length();

        for (int index = 0; index < dataLength; index++) {
            String operation = (String) dataArray.getJSONObject(index).get("kind"); //operation
            String table = (String) dataArray.getJSONObject(index).get("table");
            if ( !table.endsWith("_log") && Operation.valueOf(operation).equals(Operation.insert) || Operation.valueOf(operation).equals(Operation.update)) {
                log.info("************Adding chargeback log**************");
                log.info("Perform log for table:{} and for operation:{}", table, operation);
                String query = getQueryStringForLog(dataArray.getJSONObject(index), table, operation,createTableQuery);
                preparedStatement.executeUpdate(createTableQuery);
                preparedStatement.addBatch(query);
            }
        }
        preparedStatement.executeBatch();
        logConnection.close();
    }

    /**
     * Get query string for ddl operation.
     *
     * @param object    Json object containing WAL log for ddl operation
     * @param table     Table on which operation is performed
     * @param operation Operation performed on table (insert/delete)
     * @return
     */
    private String getQueryStringForLog(JSONObject object, String table, String operation, String createTableQuery) {
        JSONArray columnTypes = object.getJSONArray("columntypes");
        JSONArray columnNames = object.getJSONArray("columnnames");
        JSONArray columnValues = object.getJSONArray("columnvalues");
        List<Object> list1 = columnNames.toList();
        List<Object> list = list1.stream()
                .map(s -> "\"" + s + "\"")
                .collect(Collectors.toList());

        list.add("\"AUDIT_ADD_DATE\"");
        list.add("\"AUDIT_PERFORMED_OPERATION\"");
        list.add("\"AUDIT_ADD_USERNAME\"");
        String columns = list.toString().replace("[", "(").replace("]", ")");
        List<Object> clist = columnValues.toList();
        String values = "";
        Map<String,String> columnMap = new HashMap<>();
        for (int j = 0; j < columnNames.length(); j++) {
            String type = (String) columnTypes.get(j);
            String val = String.valueOf(clist.get(j));
            columnMap.put(columnNames.get(j).toString(),type);
            switch (type) {
                case "bigint":
                    values = StringUtils.isNotBlank(values) ? values.concat("," + val) : values.concat(val);
                    break;
                case "name":
                    values = StringUtils.isNotBlank(values) ? values.concat(",\'" + val + "\'") : values.concat("\'" + val + "\'");
                    break;
                case "character varying":
                    values = StringUtils.isNotBlank(values) ? values.concat(",\'" + val + "\'") : values.concat("\'" + val + "\'");
                    break;
                case "numeric":
                    values = StringUtils.isNotBlank(values) ? values.concat("," + val) : values.concat(val);
                    break;
                case "date":
                    values = StringUtils.isNotBlank(values) ? values.concat(",\'" + val + "\'") : values.concat("\'" + val + "\'");
                    break;
                case "timestamp":
                    values = StringUtils.isNotBlank(values) ? values.concat(",\'" + val + "\'") : values.concat("\'" + val + "\'");
                    break;
            }
        }

        values = "(" + values.concat(",\'" + Date.from(Instant.now()) + "\'").concat(",\'" + operation + "\'").concat(",\'system\'") + ")";
        log.info("Columns:{} and Values:{}", columns, values);
        columnMap.put("AUDIT_ADD_DATE","timestamp");
        columnMap.put("AUDIT_PERFORMED_OPERATION","character varying(30)");
        columnMap.put("AUDIT_ADD_USERNAME","character varying(30)");

        createTableQuery = createTableIfNotExist(table,columnMap);

        //String tmpQuery = String.format(" insert into %s %s values %", table, columns.toLowerCase(), values);
        //log.info(tmpQuery);

        String query = "insert into " + table + "_log" + columns.toLowerCase() + " values " + values;
        return query;
    }

    private String createTableIfNotExist(String tableName, Map<String, String> columnMap) {
        StringBuilder tableQueryBuilder = new StringBuilder("create table if not exists " + tableName+ "( \"ID\" bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),");
        for (Map.Entry<String, String> map : columnMap.entrySet()) {
            log.info(map.getKey() + " " + map.getValue() + ",");
            tableQueryBuilder.append(map.getKey().toUpperCase() + " " + map.getValue().toUpperCase() + ",");
        }
        createTableQuery = tableQueryBuilder.toString();
        if (createTableQuery.endsWith(",")) {
            createTableQuery = createTableQuery.substring(0, createTableQuery.length() - 1);
        }
        createTableQuery = createTableQuery + ", CONSTRAINT "+tableName+"_pkey PRIMARY KEY (\"ID\"));";
        log.info("Table insert query: {}", createTableQuery);
        return createTableQuery;
    }

    private Properties getProperties(){
        Properties props = new Properties();
        PGProperty.USER.set(props, appConfig.getUsername());
        PGProperty.PASSWORD.set(props, appConfig.getPassword());
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "11");
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");
        return props;
    }

}


