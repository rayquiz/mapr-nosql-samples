package maprdb.app;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.grallandco.demos.mapr.util.DBUtility;

public class PerfTest {
    private static final String COLUMN_FAMILY = "columns";
    private static final String TABLE_NAME = "/hbase/testperf";

    public static void main(final String[] args) throws IOException {
        // mvn package -Dmaven.test.skip && scp ./target/simple-java-app-1.0-SNAPSHOT.jar
        // mapr@platine-datalake01:/mapr/platine.actionlogement.com/user/mapr/testhbase/dependencies_0_98_9/

        // createTable();
        searchInTable();
    }

    public static void searchInTable() throws IOException {

        System.out.println("Lecture du CSV ...");
        final List<String> cles = new ArrayList<>(1000000);
        try (CSVParser parser = readFile()) {
            for (final CSVRecord csvRecord : parser) {
                cles.add(csvRecord.get("ID_PROPOSITION"));
            }
        }

        System.out.println("Fin de lecture du CSV");

        System.out.println("Requetage de HBase ...");
        int i = 1;
        final long start = System.currentTimeMillis();
        final Random rand = new Random();
        while (i <= 100000) {
            final int index = rand.nextInt(cles.size());
            final Map<String, String> result = DBUtility.getRow(TABLE_NAME, cles.get(index));
            // System.out.println("rowkey:" + index);
            if (i % 1000 == 0) {
                System.out.println("i:" + i);
            }
            i++;
        }
        final long end = System.currentTimeMillis();
        System.out.println("Fin de requetage de HBase");
        System.out.println(String.format("Temps total: %sms, %ss", end - start, (end - start) / 1000));
        System.out.println(
                String.format("Temps par get: %sms, %ss", (end - start) / i, (end - start) / i / 1000));

    }

    public static CSVParser readFile() throws IOException {
        final String inputCsvFile = "/mapr/platine.actionlogement.com/user/mapr/testhbase/Astria_LOCATIF_PROD_PROPOSITION.csv";

        final Reader reader = new InputStreamReader(new FileInputStream(inputCsvFile),
                StandardCharsets.UTF_8);

        final CSVFormat csvFormat = CSVFormat.newFormat('Ñ').withQuote('Õ').withRecordSeparator("\n")
                .withIgnoreEmptyLines().withHeader();
        return new CSVParser(reader, csvFormat);
    }

    public static void createTable() throws FileNotFoundException {
        final ExecutorService executor = Executors.newFixedThreadPool(10);

        try (CSVParser parser = readFile()) {
            System.out.println("Headers map: " + parser.getHeaderMap());

            DBUtility.deleteTable(TABLE_NAME);
            DBUtility.createTable(TABLE_NAME, COLUMN_FAMILY);

            for (final CSVRecord csvRecord : parser) {
                final long recordNumber = parser.getRecordNumber();
                executor.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        final String rowKey = csvRecord.get("ID_PROPOSITION");
                        DBUtility.put(TABLE_NAME, rowKey, COLUMN_FAMILY, csvRecord.toMap());
                        System.out.println("Ligne " + recordNumber);
                        return null;
                    }
                });
            }

            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.HOURS);
            System.out.println("Fin du traitement");

        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

}
