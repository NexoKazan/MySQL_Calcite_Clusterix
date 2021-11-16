import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;

import javax.sql.DataSource;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.io.BufferedWriter;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;


public class Main {

    //region DefaultArgs
    Map<String, String> defaultArgs = new HashMap() {

    };

    //endregion
    public static void main(String[] args) throws Exception {

        long start = System.currentTimeMillis();
        Map<String, String> params = SplitArgs(args);
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        String MYSQL_SCHEMA = params.get("Schema");
        String queryText = params.get("Query");
        DataSource mysqlDataSource = JdbcSchema.dataSource(
                params.get("DataSourceUrl"),
                params.get("DriverClassName"), // Change this if you want to use something like MySQL, Oracle, etc.
                params.get("Username"), // username
                params.get("Password")      // password
        );

        rootSchema.add(MYSQL_SCHEMA, JdbcSchema.create(rootSchema, MYSQL_SCHEMA, mysqlDataSource, null, null));
        SqlParser.Config parserConfig = SqlParser.config().withCaseSensitive(false);
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .parserConfig(parserConfig)
                .context(Contexts.of(calciteConnection.config()))
                .build();

        RelNode node = parseAndValidateSQL(config, queryText);
        HepProgram program = HepProgram.builder().build();
        HepPlanner planner = new HepPlanner(program);
        planner.setRoot(node);
        RelNode optimizedNode = planner.findBestExp();
        final RelRunner runner = connection.unwrap(RelRunner.class);
        PreparedStatement ps = runner.prepareStatement(optimizedNode);
        ps.setFetchSize(100);
        ps.setFetchDirection(ResultSet.FETCH_FORWARD);

        System.err.println("Prepare time: " +
                (System.currentTimeMillis() - start));
        start = System.currentTimeMillis();

        ResultSet resultSet = ps.executeQuery();

        System.err.println("Execute time: " +
                (System.currentTimeMillis() - start));
        start = System.currentTimeMillis();

        int columnCount = resultSet.getMetaData().getColumnCount();
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new
         FileOutputStream(java.io.FileDescriptor.out)), 65536);
        StringBuilder sb = new StringBuilder(4096);
        while (resultSet.next()){
            for (int i=1; i <= columnCount; i++){
                sb.append(resultSet.getString(i));
                if (i < columnCount)
                    sb.append(";");
            }
            //Thread.sleep(1000);
            sb.append("\n");
            if (sb.length()>2048)
            {
                out.write(sb.toString());
                sb.setLength(0);
            }
        }
        if (sb.length()>0)
        {
            out.write(sb.toString());
        }
        System.err.println("Write time: " +
                (System.currentTimeMillis() - start));

    }

    private static Map<String, String> SplitArgs(String[] args) {
        Map<String, String> output = new HashMap<>();
        if(args[0]!= null && args.length == 1) {
            String[] arguments = args[0].split(";");
            for (int i=0; i < arguments.length; i++) {
                String arg = arguments[i];
                int separatorIndex = arg.indexOf('=');
                String name = arg.substring(0,separatorIndex);
                String value = "";
                if (arg.length()>separatorIndex)
                {
                    value = arg.substring(separatorIndex+1);
                }
                output.put(name, value);
            }

            output = SetDefaults(output);
        }
        return output;
    }

    private static Map<String, String> SetDefaults(Map<String, String> output) {
        if(!output.containsKey("DataSourceUrl"))
        {
            output.put("DataSourceUrl", "jdbc:mysql://localhost/tpch_0?useCursorFetch=true&defaultFetchSize=10000");
        }
        if(!output.containsKey("Username"))
        {
            output.put("Username", "root");
        }
        if(!output.containsKey("Password"))
        {
            output.put("Password", "");
        }
        if(!output.containsKey("Schema"))
        {
            output.put("Schema", "s");
        }
        if(!output.containsKey("Query"))
        {
            output.put("Query", "SELECT * FROM s.nation");
        }
        if(!output.containsKey("DriverClassName"))
        {
            output.put("DriverClassName", "com.mysql.cj.jdbc.Driver");
        }
        return output;
    }

    private static RelNode parseAndValidateSQL(FrameworkConfig config, String queryText) throws SqlParseException, ValidationException, RelConversionException {
        // Build our connection
        RelNode outputRelNode;

        Planner planner = Frameworks.getPlanner(config);

        SqlNode node = planner.parse(queryText);
        SqlNode validateNode = planner.validate(node);
        RelRoot root  = planner.rel(validateNode);
        outputRelNode = root.project();
        return outputRelNode ;
    }

}
