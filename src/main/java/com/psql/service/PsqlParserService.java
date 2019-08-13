package com.psql.service;

import com.psql.AppConfig;
import org.apache.commons.lang.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.sql.DriverManager.getConnection;

@Service
public class PsqlParserService {

    @Autowired
    private AppConfig appConfig;

    private static final Logger log = LoggerFactory.getLogger(PsqlParserService.class);


    private enum DataType {bigint, name, date}

    private enum Operation {insert, update, delete}


    /**
     * Parse data obtained from WAL
     *
     * @throws SQLException
     * @throws InterruptedException
     */
    public void parseData() throws SQLException, InterruptedException {
        Properties props = new Properties();
        PGProperty.USER.set(props, appConfig.getUsername());
        PGProperty.PASSWORD.set(props, appConfig.getPassword());
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");

        try (Connection replicationConnection = getConnection(appConfig.getUrl(), props)) {
            try (Connection logConnection = getConnection(appConfig.getUrl(), props)) {

                try (Statement preparedStatement = logConnection.createStatement()) {
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
                            addLogForData(data, preparedStatement);
                        }
                        //feedback
                        stream.setAppliedLSN(stream.getLastReceiveLSN());
                        stream.setFlushedLSN(stream.getLastReceiveLSN());
                    }
                }
            }
        }
    }

    /**
     * Add log for ddl operations.
     *
     * @param data              data obtained from WAL
     * @param preparedStatement
     * @throws SQLException
     */
    private void addLogForData(String data, Statement preparedStatement) throws SQLException {
        JSONObject dataObject = new JSONObject(data);
        JSONArray dataArray = dataObject.getJSONArray("change");
        for (int i = 0; i < dataArray.length(); i++) {
            String operation = (String) dataArray.getJSONObject(i).get("kind"); //operation
            String table = (String) dataArray.getJSONObject(i).get("table");
            if (table.equalsIgnoreCase("chargeback") && Operation.valueOf(operation).equals(Operation.insert) || Operation.valueOf(operation).equals(Operation.update)) {
                log.info("************Adding chargeback log**************");
                log.info("Perform log for table:{} and for operation:{}", table, operation);
                String query = getQueryStringForLog(dataArray.getJSONObject(i), table, operation);
                preparedStatement.addBatch(query);
            }
        }
        preparedStatement.executeBatch();
    }

    /**
     * Get query string for ddl operation.
     *
     * @param object    Json object containing WAL log for ddl operation
     * @param table     Table on which operation is performed
     * @param operation Operation performed on table (insert/delete)
     * @return
     */
    private String getQueryStringForLog(JSONObject object, String table, String operation) {
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
        for (int j = 0; j < columnNames.length(); j++) {
            String type = (String) columnTypes.get(j);
            String val = String.valueOf(clist.get(j));
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
        String query = "insert into " + table + "_log " + columns + " values " + values;
        return query;
    }

}


