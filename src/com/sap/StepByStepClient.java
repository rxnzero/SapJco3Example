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
 * JAVA <> ABAP 연동 간단 예제
 */
public class StepByStepClient {
 
    static String ABAP_AS = "ABAP_AS_WITHOUT_POOL";
    static String ABAP_AS_POOLED = "ABAP_AS_WITH_POOL";
    static String ABAP_MS = "ABAP_MS_WITHOUT_POOL"; // Use Message Server
 
    // Properties 설정
    static {
        Properties connectProperties = new Properties();
        connectProperties.setProperty(DestinationDataProvider.JCO_ASHOST, "TEST"); // 호스트
        connectProperties.setProperty(DestinationDataProvider.JCO_SYSNR, "TEST"); // 시스템 번호
        connectProperties.setProperty(DestinationDataProvider.JCO_CLIENT, "TEST"); // 클라이언트 번호
        connectProperties.setProperty(DestinationDataProvider.JCO_USER, "TEST"); // 계정
        connectProperties.setProperty(DestinationDataProvider.JCO_PASSWD, "TEST"); // 암호
        connectProperties.setProperty(DestinationDataProvider.JCO_LANG, "KO"); // 언어
        createDestinationDataFile(ABAP_AS, connectProperties);
 
        connectProperties.setProperty(DestinationDataProvider.JCO_POOL_CAPACITY, "3"); // 대상에서 열린 상태로 유지되는 최대 유휴 연결
                                                                                        // 개수입니다. Default = 1
 
        connectProperties.setProperty(DestinationDataProvider.JCO_PEAK_LIMIT, "10"); // 대상에 대해 동시에 만들 수 있는 최대 활성 연결
                                                                                        // 개수입니다. Default = 0(무제한)
 
        createDestinationDataFile(ABAP_AS_POOLED, connectProperties);
 
        // 메시지 서버 세팅
        Properties connectProperties2 = new Properties();
        connectProperties2.setProperty(DestinationDataProvider.JCO_MSHOST, "TEST"); // 메시지 서버
        connectProperties2.setProperty(DestinationDataProvider.JCO_MSSERV, "TEST"); // 메시지 서버이름 OR 포트번호
        connectProperties2.setProperty(DestinationDataProvider.JCO_R3NAME, "TEST"); // 시스템 ID (SID)
        connectProperties2.setProperty(DestinationDataProvider.JCO_CLIENT, "TEST"); // 클라이언트 번호
        connectProperties2.setProperty(DestinationDataProvider.JCO_USER, "TEST"); // 게정
        connectProperties2.setProperty(DestinationDataProvider.JCO_PASSWD, "TEST"); // 암호
        connectProperties2.setProperty(DestinationDataProvider.JCO_GROUP, ""); // 그룹 - SAP 애플리케이션 서버 집합을 식별
        connectProperties2.setProperty(DestinationDataProvider.JCO_LANG, "KO"); // 언어
    }
 
    // 연결정보 파일 생성
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
     * JCO 이용하여 AS ABAP에 대한 연결을 구성하는 예
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
     * 통합 모듈을 호출하여 기능 실행 및 구조에 액세스 하는 함수
     * 
     * Executing Simple Functions
     * 
     * @throws JCoException
     */
    public static void step3SimpleCall() throws JCoException {
        // 연결정보 호출
        JCoDestination destination = JCoDestinationManager.getDestination(ABAP_AS_POOLED);
        // STFC_CONNECTION 함수 호출
        JCoFunction function = destination.getRepository().getFunction("STFC_CONNECTION");
 
        // 함수가 없으면 throw Exception
        if (function == null)
            throw new RuntimeException("BAPI_COMPANYCODE_GETLIST not found in SAP.");
 
        // Import 파라메터 입력
        function.getImportParameterList().setValue("REQUTEXT", "Hello SAP");
 
        try {
            // 지정한 함수 실행
            function.execute(destination);
        } catch (AbapException e) {
            System.out.println(e.toString());
            return;
        }
 
        System.out.println("STFC_CONNECTION finished:");
        // Export 파라메터 출력
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
 
        // 구조체로 선언된 타입 호출
        JCoStructure exportStructure = function.getExportParameterList().getStructure("RFCSI_EXPORT");
        // 연결정보의 시스템 ID 출력하는 부분
        System.out.println("System info for " + destination.getAttributes().getSystemID() + ":\n");
 
        // 구조체 컬럼 수 만큼 반복
        for (int i = 0; i < exportStructure.getMetaData().getFieldCount(); i++) {
            System.out.println(exportStructure.getMetaData().getName(i) + ":\t" + exportStructure.getString(i));
            // 컬럼네임 : 값 형식으로 출력될거라고 예상
        }
        System.out.println();
 
        // JCo still supports the JCoFields, but direct access via getXXX is more
        // efficient as field iterator
        System.out.println("The same using field iterator: \nSystem info for "
                + destination.getAttributes().getSystemID() + ":\n");
        // 위에 내용 foreach 로 구현한 것
        for (JCoField field : exportStructure) {
            System.out.println(field.getName() + ":\t" + field.getString());
            // 위에서 보면 컬럼명 가져올 때 getMetaData 후 가져오는데 차이가 있을진 모르겠음.
        }
        System.out.println();
    }
 
    /**
     * 테이블 접근
     * 
     * 회사 코드를 불러오는 예제.
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
 
        // COMPANYCODE_LIST 테이블 불러옴
        JCoTable codes = function.getTableParameterList().getTable("COMPANYCODE_LIST");
        // 테이블 로우 수 만큼 반복
        for (int i = 0; i < codes.getNumRows(); i++) {
            // 테이블 로우 커서 세팅
            codes.setRow(i);
            // 테이블 로우 별 값 프린트
            System.out.println(codes.getString("COMP_CODE") + '\t' + codes.getString("COMP_NAME"));
        }
 
        // 테이블 첫 커서로 이동
        codes.firstRow();
        // 로우수 만큼 반복, 반복 시 테이블 커서 다음 로우로 이동
        for (int i = 0; i < codes.getNumRows(); i++, codes.nextRow()) {
            function = destination.getRepository().getFunction("BAPI_COMPANYCODE_GETDETAIL");
            if (function == null)
                throw new RuntimeException("BAPI_COMPANYCODE_GETDETAIL not found in SAP.");
 
            // Import 파라메터 설정, 위에서 불러온 테이블 값으로
            function.getImportParameterList().setValue("COMPANYCODEID", codes.getString("COMP_CODE"));
 
            // 사용하지 않는 파라메터 비활성화
            // 비활성 파라미터는 생성되지 않거나 생성되어도 비활성화 상태로 변환이 된다?
            // (원문: Inactive parameters will be either not generated or at least converted.)
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
     * 간단한 stateful 호출 예제.
     * 
     * 동일한 컨텍스트에서 여러 함수를 호출하려면 stateful 연결이 필요합니다. stateful 연결 사용 시
     * SessionReferenceProvider 를 구형해야합니다. 아래 예제는 동일 스레드를 이용한 간단한 예제이므로
     * SessionReferenceProvider 를 구현할 필요는 없습니다.
     * 
     * SessionReferenceProvider 이용: MultiThreadedExample.java 참조
     * 
     * @throws JCoException
     */
    public static void step4SimpleStatefulCalls() throws JCoException {
        // Function Template 선언
        final JCoFunctionTemplate incrementCounterTemplate, getCounterTemplate;
 
        JCoDestination destination = JCoDestinationManager.getDestination(ABAP_MS);
        // 템플릿에 사용할 기능 지정
        incrementCounterTemplate = destination.getRepository().getFunctionTemplate("Z_INCREMENT_COUNTER");
        getCounterTemplate = destination.getRepository().getFunctionTemplate("Z_GET_COUNTER");
        if (incrementCounterTemplate == null || getCounterTemplate == null)
            throw new RuntimeException(
                    "This example cannot run without Z_INCREMENT_COUNTER and Z_GET_COUNTER functions");
 
        // 스레드 숫자
        final int threadCount = 5;
        final int loops = 5;
 
        // 래치 선언 (쓸모없어보임;;)
        final CountDownLatch startSignal = new CountDownLatch(threadCount);
        final CountDownLatch doneSignal = new CountDownLatch(threadCount);
 
        Runnable worker = new Runnable() {
            public void run() {
                startSignal.countDown(); // startSignal--
                try {
                    // wait for other threads
                    startSignal.await(); // startSignal 0까지 대기
 
                    // startSignal 끝나면 시작
                    JCoDestination dest = JCoDestinationManager.getDestination(ABAP_MS);
                    // stateful 연결 사용 시 begin(destination), end(destination) 사용  
                    JCoContext.begin(dest);
                    try {
                        for (int i = 0; i < loops; i++) {
                            // Z_INCREMENT_COUNTER 함수
                            JCoFunction incrementCounter = incrementCounterTemplate.getFunction();
                            incrementCounter.execute(dest);
                        }
                        // Z_GET_COUNTER 함수
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
        // ZSBDTI_RFC_DOC_LIST 함수 연결
        JCoFunction function = destination.getRepository().getFunction("RFC_TEST");
 
        if (function == null)
            throw new RuntimeException("RFC_TEST not found in SAP.");
 
        // T_CONDITION 테이블 가져옴
        JCoTable importTable = function.getTableParameterList().getTable("T_INPUT");
 
        if (importTable == null) {
            System.out.println("[T_INPUT] not found in SAP");
            return;
 
        }
 
        // table row 추가는 jcoTable.appendRow(); 로 가능
        // value 지정 시 SAP 에서 지정한 데이터타입과 같아야함. (틀릴 시 에러발생)
        importTable.appendRow();
        importTable.setValue("DATE_FR", "20200101"); // D(8)
        importTable.setValue("DATE_TO", "20201231"); // D(8)
        importTable.setValue("USER_NAME", "KSH"); // C(10)
        importTable.setValue("CODE", 2000); // N(4)
        importTable.setValue("TITLE", "Test Document"); // C(200)
 
        printTable(importTable);
 
        // 실행
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
     * JCoTable 출력 함수
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