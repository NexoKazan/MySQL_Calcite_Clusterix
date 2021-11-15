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


public class Main {

    //region DefaultArgs
    Map<String, String> defaultArgs = new HashMap() {

    };

    //endregion
    public static void main(String[] args) throws Exception {
        Map<String, String> params = SplitArgs(args);
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        String MYSQL_SCHEMA = params.get("Schema");
        String queryText = params.get("Query");
        DataSource mysqlDataSource = JdbcSchema.dataSource(
                "jdbc:" + params.get("Driver") + "://" + params.get("Server") + "/" + params.get("Database"),
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
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()){
            for (int i=1; i <= resultSet.getMetaData().getColumnCount(); i++){
                System.out.print(resultSet.getString(i));
                System.out.print(";");
            }
            //Thread.sleep(1000);
            System.out.println();
        }

    }

    private static Map<String, String> SplitArgs(String[] args) {
        if(args[0]!= null && args.length == 1) {
            Map<String, String> output = new HashMap<>();
            String[] arguments = args[0].split(";");
            for (int i=0; i < arguments.length; i++
                 ) {
                String[] NameValue = arguments[i].split("=");
                output.put(NameValue[0], NameValue[1]);
            }
            output = SetDefaults(output);
            return output;
        }
        else{
            return null;
        }
    }

    private static Map<String, String> SetDefaults(Map<String, String> output) {
        if(!output.containsKey("Server"))
        {
            output.put("Server", "localhost");
        }
        if(!output.containsKey("Database"))
        {
            output.put("Database", "tpch_0");
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
        if(!output.containsKey("Driver"))
        {
            output.put("Driver", "mysql");
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
