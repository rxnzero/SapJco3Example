package com.sap;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import com.sap.conn.jco.AbapException;
import com.sap.conn.jco.JCoContext;
import com.sap.conn.jco.JCoDestination;
import com.sap.conn.jco.JCoDestinationManager;
import com.sap.conn.jco.JCoException;
import com.sap.conn.jco.JCoField;
import com.sap.conn.jco.JCoFunction;
import com.sap.conn.jco.JCoFunctionTemplate;
import com.sap.conn.jco.JCoStructure;
import com.sap.conn.jco.JCoTable;
import com.sap.conn.jco.ext.DestinationDataProvider;
 
/**
 * JAVA <> ABAP ���� ���� ����
 */
public class StepByStepClient {
 
    static String ABAP_AS = "ABAP_AS_WITHOUT_POOL";
    static String ABAP_AS_POOLED = "ABAP_AS_WITH_POOL";
    static String ABAP_MS = "ABAP_MS_WITHOUT_POOL"; // Use Message Server
 
    // Properties ����
    static {
        Properties connectProperties = new Properties();
        connectProperties.setProperty(DestinationDataProvider.JCO_ASHOST, "TEST"); // ȣ��Ʈ
        connectProperties.setProperty(DestinationDataProvider.JCO_SYSNR, "TEST"); // �ý��� ��ȣ
        connectProperties.setProperty(DestinationDataProvider.JCO_CLIENT, "TEST"); // Ŭ���̾�Ʈ ��ȣ
        connectProperties.setProperty(DestinationDataProvider.JCO_USER, "TEST"); // ����
        connectProperties.setProperty(DestinationDataProvider.JCO_PASSWD, "TEST"); // ��ȣ
        connectProperties.setProperty(DestinationDataProvider.JCO_LANG, "KO"); // ���
        createDestinationDataFile(ABAP_AS, connectProperties);
 
        connectProperties.setProperty(DestinationDataProvider.JCO_POOL_CAPACITY, "3"); // ��󿡼� ���� ���·� �����Ǵ� �ִ� ���� ����
                                                                                        // �����Դϴ�. Default = 1
 
        connectProperties.setProperty(DestinationDataProvider.JCO_PEAK_LIMIT, "10"); // ��� ���� ���ÿ� ���� �� �ִ� �ִ� Ȱ�� ����
                                                                                        // �����Դϴ�. Default = 0(������)
 
        createDestinationDataFile(ABAP_AS_POOLED, connectProperties);
 
        // �޽��� ���� ����
        Properties connectProperties2 = new Properties();
        connectProperties2.setProperty(DestinationDataProvider.JCO_MSHOST, "TEST"); // �޽��� ����
        connectProperties2.setProperty(DestinationDataProvider.JCO_MSSERV, "TEST"); // �޽��� �����̸� OR ��Ʈ��ȣ
        connectProperties2.setProperty(DestinationDataProvider.JCO_R3NAME, "TEST"); // �ý��� ID (SID)
        connectProperties2.setProperty(DestinationDataProvider.JCO_CLIENT, "TEST"); // Ŭ���̾�Ʈ ��ȣ
        connectProperties2.setProperty(DestinationDataProvider.JCO_USER, "TEST"); // ����
        connectProperties2.setProperty(DestinationDataProvider.JCO_PASSWD, "TEST"); // ��ȣ
        connectProperties2.setProperty(DestinationDataProvider.JCO_GROUP, ""); // �׷� - SAP ���ø����̼� ���� ������ �ĺ�
        connectProperties2.setProperty(DestinationDataProvider.JCO_LANG, "KO"); // ���
    }
 
    // �������� ���� ����
    static void createDestinationDataFile(String destinationName, Properties connectProperties) {
        File destCfg = new File(destinationName + ".jcoDestination");
        try {
            FileOutputStream fos = new FileOutputStream(destCfg, false);
            connectProperties.store(fos, "for tests only !");
            fos.close();
        } catch (Exception e) {
            throw new RuntimeException("Unable to create thedestination files", e);
        }
    }
 
    /**
     * JCO �̿��Ͽ� AS ABAP�� ���� ������ �����ϴ� ��
     * 
     * Direct Connection to an AS ABAP
     * 
     * @throws JCoException
     */
    public static void step1Connect() throws JCoException {
        JCoDestination destination = JCoDestinationManager.getDestination(ABAP_AS);
        System.out.println("Attributes:");
        System.out.println(destination.getAttributes());
        System.out.println();
 
        destination = JCoDestinationManager.getDestination(ABAP_MS);
        System.out.println("Attributes:");
        System.out.println(destination.getAttributes());
        System.out.println();
    }
 
    /**
     * Pool Connection to an AS ABAP
     * 
     * @throws JCoException
     */
    public static void step2ConnectUsingPool() throws JCoException {
        JCoDestination destination = JCoDestinationManager.getDestination(ABAP_AS_POOLED);
        destination.ping();
        System.out.println("Attributes:");
        System.out.println(destination.getAttributes());
        System.out.println();
    }
 
    /**
     * ���� ����� ȣ���Ͽ� ��� ���� �� ������ �׼��� �ϴ� �Լ�
     * 
     * Executing Simple Functions
     * 
     * @throws JCoException
     */
    public static void step3SimpleCall() throws JCoException {
        // �������� ȣ��
        JCoDestination destination = JCoDestinationManager.getDestination(ABAP_AS_POOLED);
        // STFC_CONNECTION �Լ� ȣ��
        JCoFunction function = destination.getRepository().getFunction("STFC_CONNECTION");
 
        // �Լ��� ������ throw Exception
        if (function == null)
            throw new RuntimeException("BAPI_COMPANYCODE_GETLIST not found in SAP.");
 
        // Import �Ķ���� �Է�
        function.getImportParameterList().setValue("REQUTEXT", "Hello SAP");
 
        try {
            // ������ �Լ� ����
            function.execute(destination);
        } catch (AbapException e) {
            System.out.println(e.toString());
            return;
        }
 
        System.out.println("STFC_CONNECTION finished:");
        // Export �Ķ���� ���
        System.out.println(" Echo: " + function.getExportParameterList().getString("ECHOTEXT"));
        System.out.println(" Response: " + function.getExportParameterList().getString("RESPTEXT"));
        System.out.println();
    }
 
    /**
     * Accessing a Structure
     * 
     * @throws JCoException
     */
    public static void step3WorkWithStructure() throws JCoException {
        JCoDestination destination = JCoDestinationManager.getDestination(ABAP_AS_POOLED);
        JCoFunction function = destination.getRepository().getFunction("RFC_SYSTEM_INFO");
        if (function == null)
            throw new RuntimeException("RFC_SYSTEM_INFO not found in SAP.");
 
        try {
            function.execute(destination);
        } catch (AbapException e) {
            System.out.println(e.toString());
            return;
        }
 
        // ����ü�� ����� Ÿ�� ȣ��
        JCoStructure exportStructure = function.getExportParameterList().getStructure("RFCSI_EXPORT");
        // ���������� �ý��� ID ����ϴ� �κ�
        System.out.println("System info for " + destination.getAttributes().getSystemID() + ":\n");
 
        // ����ü �÷� �� ��ŭ �ݺ�
        for (int i = 0; i < exportStructure.getMetaData().getFieldCount(); i++) {
            System.out.println(exportStructure.getMetaData().getName(i) + ":\t" + exportStructure.getString(i));
            // �÷����� : �� �������� ��µɰŶ�� ����
        }
        System.out.println();
 
        // JCo still supports the JCoFields, but direct access via getXXX is more
        // efficient as field iterator
        System.out.println("The same using field iterator: \nSystem info for "
                + destination.getAttributes().getSystemID() + ":\n");
        // ���� ���� foreach �� ������ ��
        for (JCoField field : exportStructure) {
            System.out.println(field.getName() + ":\t" + field.getString());
            // ������ ���� �÷��� ������ �� getMetaData �� �������µ� ���̰� ������ �𸣰���.
        }
        System.out.println();
    }
 
    /**
     * ���̺� ����
     * 
     * ȸ�� �ڵ带 �ҷ����� ����.
     * 
     * @throws JCoException
     */
    public static void step4WorkWithTable() throws JCoException {
        JCoDestination destination = JCoDestinationManager.getDestination(ABAP_AS_POOLED);
        JCoFunction function = destination.getRepository().getFunction("BAPI_COMPANYCODE_GETLIST");
        if (function == null)
            throw new RuntimeException("BAPI_COMPANYCODE_GETLIST not found in SAP.");
 
        try {
            function.execute(destination);
        } catch (AbapException e) {
            System.out.println(e.toString());
            return;
        }
 
        JCoStructure returnStructure = function.getExportParameterList().getStructure("RETURN");
 
        // TYPE != "" AND TYPE != "S"
        if (!(returnStructure.getString("TYPE").equals("") || returnStructure.getString("TYPE").equals("S"))) {
            throw new RuntimeException(returnStructure.getString("MESSAGE"));
        }
 
        // COMPANYCODE_LIST ���̺� �ҷ���
        JCoTable codes = function.getTableParameterList().getTable("COMPANYCODE_LIST");
        // ���̺� �ο� �� ��ŭ �ݺ�
        for (int i = 0; i < codes.getNumRows(); i++) {
            // ���̺� �ο� Ŀ�� ����
            codes.setRow(i);
            // ���̺� �ο� �� �� ����Ʈ
            System.out.println(codes.getString("COMP_CODE") + '\t' + codes.getString("COMP_NAME"));
        }
 
        // ���̺� ù Ŀ���� �̵�
        codes.firstRow();
        // �ο�� ��ŭ �ݺ�, �ݺ� �� ���̺� Ŀ�� ���� �ο�� �̵�
        for (int i = 0; i < codes.getNumRows(); i++, codes.nextRow()) {
            function = destination.getRepository().getFunction("BAPI_COMPANYCODE_GETDETAIL");
            if (function == null)
                throw new RuntimeException("BAPI_COMPANYCODE_GETDETAIL not found in SAP.");
 
            // Import �Ķ���� ����, ������ �ҷ��� ���̺� ������
            function.getImportParameterList().setValue("COMPANYCODEID", codes.getString("COMP_CODE"));
 
            // ������� �ʴ� �Ķ���� ��Ȱ��ȭ
            // ��Ȱ�� �Ķ���ʹ� �������� �ʰų� �����Ǿ ��Ȱ��ȭ ���·� ��ȯ�� �ȴ�?
            // (����: Inactive parameters will be either not generated or at least converted.)
            function.getExportParameterList().setActive("COMPANYCODE_ADDRESS", false);
 
            try {
                function.execute(destination);
            } catch (AbapException e) {
                System.out.println(e.toString());
                return;
            }
 
            returnStructure = function.getExportParameterList().getStructure("RETURN");
            // TYPE != "" AND TYPE != "S" AND TYPE != "W"
            if (!(returnStructure.getString("TYPE").equals("") || returnStructure.getString("TYPE").equals("S")
                    || returnStructure.getString("TYPE").equals("W"))) {
                throw new RuntimeException(returnStructure.getString("MESSAGE"));
            }
 
            JCoStructure detail = function.getExportParameterList().getStructure("COMPANYCODE_DETAIL");
 
            System.out.println(detail.getString("COMP_CODE") + '\t' + detail.getString("COUNTRY") + '\t'
                    + detail.getString("CITY"));
        } // for
    }
 
    /**
     * 
     * ������ stateful ȣ�� ����.
     * 
     * ������ ���ؽ�Ʈ���� ���� �Լ��� ȣ���Ϸ��� stateful ������ �ʿ��մϴ�. stateful ���� ��� ��
     * SessionReferenceProvider �� �����ؾ��մϴ�. �Ʒ� ������ ���� �����带 �̿��� ������ �����̹Ƿ�
     * SessionReferenceProvider �� ������ �ʿ�� �����ϴ�.
     * 
     * SessionReferenceProvider �̿�: MultiThreadedExample.java ����
     * 
     * @throws JCoException
     */
    public static void step4SimpleStatefulCalls() throws JCoException {
        // Function Template ����
        final JCoFunctionTemplate incrementCounterTemplate, getCounterTemplate;
 
        JCoDestination destination = JCoDestinationManager.getDestination(ABAP_MS);
        // ���ø��� ����� ��� ����
        incrementCounterTemplate = destination.getRepository().getFunctionTemplate("Z_INCREMENT_COUNTER");
        getCounterTemplate = destination.getRepository().getFunctionTemplate("Z_GET_COUNTER");
        if (incrementCounterTemplate == null || getCounterTemplate == null)
            throw new RuntimeException(
                    "This example cannot run without Z_INCREMENT_COUNTER and Z_GET_COUNTER functions");
 
        // ������ ����
        final int threadCount = 5;
        final int loops = 5;
 
        // ��ġ ���� (��������;;)
        final CountDownLatch startSignal = new CountDownLatch(threadCount);
        final CountDownLatch doneSignal = new CountDownLatch(threadCount);
 
        Runnable worker = new Runnable() {
            public void run() {
                startSignal.countDown(); // startSignal--
                try {
                    // wait for other threads
                    startSignal.await(); // startSignal 0���� ���
 
                    // startSignal ������ ����
                    JCoDestination dest = JCoDestinationManager.getDestination(ABAP_MS);
                    // stateful ���� ��� �� begin(destination), end(destination) ���  
                    JCoContext.begin(dest);
                    try {
                        for (int i = 0; i < loops; i++) {
                            // Z_INCREMENT_COUNTER �Լ�
                            JCoFunction incrementCounter = incrementCounterTemplate.getFunction();
                            incrementCounter.execute(dest);
                        }
                        // Z_GET_COUNTER �Լ�
                        JCoFunction getCounter = getCounterTemplate.getFunction();
                        getCounter.execute(dest);
 
                        int remoteCounter = getCounter.getExportParameterList().getInt("GET_VALUE");
                        System.out.println("Thread-" + Thread.currentThread().getId() + " finished. Remote counter has "
                                + (loops == remoteCounter ? "correct" : "wrong") + " value [" + remoteCounter + "]");
                    } finally {
                        JCoContext.end(dest);
                    }
                } catch (Exception e) {
                    System.out.println(
                            "Thread-" + Thread.currentThread().getId() + " ends with exception " + e.toString());
                }
 
                doneSignal.countDown();
            }
        };
 
        for (int i = 0; i < threadCount; i++) {
            new Thread(worker).start();
        }
 
        try {
            doneSignal.await();
        } catch (Exception e) {
        }
 
    }
 
    public static void main(String[] args) throws JCoException {
        step1Connect();
        step2ConnectUsingPool();
        step3SimpleCall();
        step4WorkWithTable();
        step4SimpleStatefulCalls();
    }
    
    /**
     * Use Table Parameter
     * 
     * @Author Shyun Kim
     * @Date 2020.08.19
     * 
     * @throws JCoException
     */
    public static void searchList() throws JCoException {
        JCoDestination destination = JCoDestinationManager.getDestination(ABAP_AS_POOLED);
        // ZSBDTI_RFC_DOC_LIST �Լ� ����
        JCoFunction function = destination.getRepository().getFunction("RFC_TEST");
 
        if (function == null)
            throw new RuntimeException("RFC_TEST not found in SAP.");
 
        // T_CONDITION ���̺� ������
        JCoTable importTable = function.getTableParameterList().getTable("T_INPUT");
 
        if (importTable == null) {
            System.out.println("[T_INPUT] not found in SAP");
            return;
 
        }
 
        // table row �߰��� jcoTable.appendRow(); �� ����
        // value ���� �� SAP ���� ������ ������Ÿ�԰� ���ƾ���. (Ʋ�� �� �����߻�)
        importTable.appendRow();
        importTable.setValue("DATE_FR", "20200101"); // D(8)
        importTable.setValue("DATE_TO", "20201231"); // D(8)
        importTable.setValue("USER_NAME", "KSH"); // C(10)
        importTable.setValue("CODE", 2000); // N(4)
        importTable.setValue("TITLE", "Test Document"); // C(200)
 
        printTable(importTable);
 
        // ����
        function.execute(destination);
 
        String rtnType = (String) function.getExportParameterList().getValue("RETURN_TYPE");
        String rtnMsg = (String) function.getExportParameterList().getValue("RETURN_MSG");
 
        System.out.println("RETURN_TYPE: \t" + rtnType);
        System.out.println("RETURN_MSG: \t" + rtnMsg);
 
        JCoTable exportTable = function.getTableParameterList().getTable("T_DOC_LIST");
 
        if (exportTable == null) {
            System.out.println("[T_DOC_LIST] not found in SAP");
            return;
        }
 
        printTable(exportTable);
    }
 
    /**
     * JCoTable ��� �Լ�
     * 
     * @Author Shyun Kim
     * @Date 2020.08.19
     * 
     * @param jcoTable
     */
    public static void printTable(JCoTable jcoTable) {
        int[] maxLengths = new int[jcoTable.getFieldCount()];
 
        for (int i = 0; i < jcoTable.getNumRows(); i++) {
            jcoTable.setRow(i);
            int j = 0;
            for (JCoField f : jcoTable) {
                if (i == 0)
                    maxLengths[j] = Math.max(maxLengths[j], f.getName().getBytes().length);
 
                maxLengths[j] = Math.max(maxLengths[j], f.getString().getBytes().length);
                j++;
            }
        }
 
        StringBuilder formatBuilder = new StringBuilder();
        for (int maxLength : maxLengths) {
            formatBuilder.append("%-").append(maxLength + 2).append("s").append("|  ");
        }
        String format = formatBuilder.toString();
 
        StringBuilder result = new StringBuilder();
 
        for (int i = -1; i < jcoTable.getNumRows(); i++) {
            jcoTable.setRow(i);
            ArrayList<String> row = new ArrayList<String>();
 
            for (JCoField f : jcoTable) {
                if (i == -1)
                    row.add(f.getName());
                else
                    row.add(f.getString());
            }
            result.append(String.format(format, row.toArray())).append("\n");
        }
        System.out.println(result.toString());
    }
}