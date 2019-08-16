package com.psql;

import org.postgresql.PGProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.util.Properties;

public class QueryExecutor  {

    int id;

    Connection con;

    Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    public QueryExecutor(int id) throws Exception {
        this.id = id;

        if(null == this.con){
            Properties props = new Properties();
            PGProperty.USER.set(props, "postgres");
            PGProperty.PASSWORD.set(props, "postgres");
            PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "11");
            PGProperty.REPLICATION.set(props, "database");
            PGProperty.PREFER_QUERY_MODE.set(props, "simple");
            this.con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/psql?currentSchema=public", props);
        }
    }


    public void insertData() {

        try {
            logger.info("%%%%%%%%%%%%%%%%%%%%%%% chargeback table entry created with id: {}", id);
            updateSchema();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.info("{}",e);
            System.out.printf("extectpifd " + e);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(null != con) {
                try {
                    con.close();
                } catch (Exception e){

                }
            }
        }
    }

    private  void updateSchema() throws SQLException {

        //insert
        String query = "insert into chargeback(\"BILL_ID\", \"BILL_TYPE_ID\", \"PARENT_BILL_ID\", \"PROCESSOR_RESPONSE_TXN_ID\", \"FEE\", \"CHARGEBACK_AMOUNT\", \"CHARGEBACK_CURRENCY\", \"RESPONSE_FILE_NAME\", \"HELIX_SOURCE_SYSTEM_ID\", \"BILL_PROCESSOR_ID\", \"CATEGORY\", \"PARTIAL_REPRESENTMENT\", \"CHARGEBACK_REASON_CODE\", \"CHARGEBACK_INITIATED_DATE\", \"CHARGEBACK_DUE_DATE\", \"CHARGEBACK_REJECT_REASON_ID\", \"MOP_CODE\", \"CHARGEBACK_STATUS_ID\", \"CHARGEBACK_SOURCE_DATA\", \"PROCESS_THREAD\", \"USER_CREATED\", \"DATE_CREATED\", \"USER_MODIFIED\", \"DATE_MODIFIED\") values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement pstmt = con.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
        pstmt.setInt(1, 1);
        pstmt.setInt(2, 2);
        pstmt.setInt(3, 3);
        pstmt.setString(4, "004");
        pstmt.setInt(5, 100);
        pstmt.setInt(6, 150);
        pstmt.setString(7, "EUR");
        pstmt.setString(8, "response_file");
        pstmt.setInt(9, 1);
        pstmt.setInt(10, 10);
        pstmt.setString(11, "Retrieval");
        pstmt.setString(12, "009");
        pstmt.setString(13, "33");
        pstmt.setString(14, Date.from(Instant.now()).toString());
        pstmt.setString(15, Date.from(Instant.now()).toString());
        pstmt.setInt(16, 1);
        pstmt.setString(17, "003");
        pstmt.setInt(18, 1);
        pstmt.setString(19, "Retrieval created via UI");
        pstmt.setString(20, "WLPP");
        pstmt.setString(21, "system");
        pstmt.setString(22, Date.from(Instant.now()).toString());
        pstmt.setString(23, "system");
        pstmt.setString(24, Date.from(Instant.now()).toString());
        pstmt.execute();

        ResultSet resultSet = pstmt.getGeneratedKeys();

        if (resultSet.next()) {
            id = resultSet.getInt(1);
        }

        /*//update name and phone
        String nm = "demo user " + (id + 2);
        PreparedStatement pstmt1 = con.prepareStatement("update chargeback set \"FEE\" = ?,\"CHARGEBACK_AMOUNT\"=? where \"CHARGEBACK_ID\" = ?");
        pstmt1.setInt(1, 150);
        pstmt1.setInt(2, 50);
        pstmt1.setInt(3, id);
        pstmt1.executeUpdate();*/

//        logger.info("%%%%%%%%%%%%%%%%%%%%%%% FEE and CHARGEBACK_AMOUNT updated to : 150 and 50");
//        //update email
//        PreparedStatement pstmt2 = con.prepareStatement("update chargeback set \"PARTIAL_REPRESENTMENT\" = ? where \"CHARGEBACK_ID\" = ?");
//        pstmt2.setString(1, "PARTIAL_REPRESENTMENT_UPDATE");
//        pstmt2.setInt(2, id);
//        pstmt2.executeUpdate();
//
//        logger.info("%%%%%%%%%%%%%%%%%%%%%%% PARTIAL_REPRESENTMENT updated to PARTIAL_REPRESENTMENT_UPDATE");
//        //update phone
//        PreparedStatement pstmt3 = con.prepareStatement("update chargeback set \"CHARGEBACK_REJECT_REASON_ID\" = ? where \"CHARGEBACK_ID\" = ?");
//        pstmt3.setInt(1, 78);
//        pstmt3.setInt(2, id);
//        pstmt3.executeUpdate();
//
//        logger.info("%%%%%%%%%%%%%%%%%%%%%%% CHARGEBACK_REJECT_REASON_ID updated to 78");
//
//        //update phone
//        PreparedStatement pstmt4 = con.prepareStatement("update chargeback set \"CHARGEBACK_REASON_CODE\" = ? where \"CHARGEBACK_ID\" = ?");
//        pstmt4.setString(1, "005");
//        pstmt4.setInt(2, id);
//        pstmt4.executeUpdate();
//
//        logger.info("%%%%%%%%%%%%%%%%%%%%%%% CHARGEBACK_REASON_CODE updated to 005");
//        //update phone
//        PreparedStatement pstmt5 = con.prepareStatement("update chargeback set \"CHARGEBACK_REASON_CODE\" = ? where \"CHARGEBACK_ID\" = ?");
//        pstmt5.setString(1, "000001");
//        pstmt5.setInt(2, id);
//        pstmt5.executeUpdate();
//
//        logger.info("%%%%%%%%%%%%%%%%%%%%%%% CHARGEBACK_REASON_CODE updated to 000001");
    }

    public static void main(String[] args) throws Exception{
        QueryExecutor queryExecutor = new QueryExecutor(1);
        queryExecutor.insertData();
    }
}