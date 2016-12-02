package maprdb.app;

import java.io.IOException;
import java.util.Map;

import com.grallandco.demos.mapr.util.DBUtility;

public class SimpleApp {

    public static void main(final String[] args) throws IOException {
        // http://stackoverflow.com/questions/34659166/connecting-to-mapr-db-m3-from-a-java-client
        if (args.length != 1) {
            System.out.println("You MUST pass the name of the table");
            System.exit(0);
        }

        final String tableName = args[0];
        final String[] cf = { "general", "details" };

        DBUtility.createTable(tableName, cf);
        DBUtility.put(tableName, "jessica_jones", "general", "first_name", "Jessica");
        DBUtility.put(tableName, "jessica_jones", "general", "last_name", "Jones");
        DBUtility.put(tableName, "jessica_jones", "general", "powers",
                "Superhuman strength and endurance, Flight, Psionic protection");
        DBUtility.put(tableName, "jessica_jones", "details", "city", "New York");
        DBUtility.put(tableName, "jessica_jones", "details", "publisher", "Marvel");

        DBUtility.put(tableName, "daredevil", "general", "first_name", "Matt");
        DBUtility.put(tableName, "daredevil", "general", "last_name", "Murdock");
        DBUtility.put(tableName, "daredevil", "general", "alias", "Daredevil");
        DBUtility.put(tableName, "daredevil", "general", "powers",
                "Peak human physical and mental condition, Highly skilled acrobat and hand-to-hand combatant, Radar sense ,Superhuman senses");
        DBUtility.put(tableName, "daredevil", "details", "city", "New York");
        DBUtility.put(tableName, "daredevil", "details", "publisher", "Marvel");

        DBUtility.put(tableName, "wolverine", "general", "first_name", "James");
        DBUtility.put(tableName, "wolverine", "general", "last_name", "Howlett");
        DBUtility.put(tableName, "wolverine", "general", "alias", "Wolverine");
        DBUtility.put(tableName, "wolverine", "general", "powers",
                "Regenerative healing factor, Adamantium-plated skeletal structure and retractable claws");
        DBUtility.put(tableName, "wolverine", "details", "city", "Cold Lake");
        DBUtility.put(tableName, "wolverine", "details", "publisher", "Marvel");

        for (final Map.Entry<String, Map<String, String>> entry : DBUtility.scan(tableName).entrySet()) {
            System.out.println(entry.getKey());
            for (final Map.Entry<String, String> row : entry.getValue().entrySet()) {
                System.out.println("\t" + row.getKey() + "=" + row.getValue());
            }
        }

    }

}
