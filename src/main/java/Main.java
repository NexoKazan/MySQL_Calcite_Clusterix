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


public class Main {

    public static void main(String[] args) throws Exception {


        /*
          args[0] - Сервер
          args[1] - База данных
          args[2] - UserName
          args[3] - password
          args[4] - название схемы
          args[5] - ТекстЗапроса

         */
        String serverIP = "localhost";
        serverIP = args[0];
        String sourceDataBaseName = "tpch_0";
        sourceDataBaseName = args[1];
        String sourceDatabaseUser = "calcite";
        sourceDatabaseUser = args[2];
        String sourceDatabasePassword = "calcite";
        sourceDatabasePassword = args[3];
        String MYSQL_SCHEMA = "m";
        MYSQL_SCHEMA = args[4];
        String queryText = "SELECT * FROM m.nation";  //Запрос всегда должен содержать в поле From указание имени схемы.
        queryText = args[5];

        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        DataSource mysqlDataSource = JdbcSchema.dataSource(
                "jdbc:mysql://"+ serverIP + "/" + sourceDataBaseName,
                "com.mysql.cj.jdbc.Driver", // Change this if you want to use something like MySQL, Oracle, etc.
                sourceDatabaseUser, // username
                sourceDatabasePassword      // password
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
