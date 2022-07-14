import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.adapter.csv.*;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;

import javax.sql.DataSource;
import java.io.*;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;


public class Main {

    //region DefaultArgsы
    Map<String, String> defaultArgs = new HashMap() {

    };

    //endregion
    public static void main(String[] args) throws Exception {

        //System.in.read();
        long start = System.currentTimeMillis();
        Map<String, String> params = SplitArgs(args);
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        String schemaName = params.get("Schema");
        String queryText = params.get("Query");
        Schema dbSchema;
        if (!params.get("DriverClassName").contains("csv")){
            DataSource mysqlDataSource = JdbcSchema.dataSource(
                    params.get("DataSourceUrl"),
                    params.get("DriverClassName"), // Change this if you want to use something like MySQL, Oracle, etc.
                    params.get("Username"), // username
                    params.get("Password")      // password
            );
            dbSchema = JdbcSchema.create(rootSchema, schemaName, mysqlDataSource, null, null);
        } else {
            dbSchema = new CsvSchema(new File(params.get("DataSourceUrl").replace("csv:","")), CsvTable.Flavor.SCANNABLE);
        }
        rootSchema.add(schemaName, dbSchema);

        SqlParser.Config parserConfig = SqlParser.config().withCaseSensitive(false);
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .parserConfig(parserConfig)
                .context(Contexts.of(calciteConnection.config()))
                .build();


        RelNode node = parseAndValidateSQL(config, queryText);

        //System.out.println(RelOptUtil.toString(node));
        RelNode optimizedNode = node;
        switch (params.get("Plan")){
            case "Hep" :
                HepProgram program = HepProgram.builder().build();
                HepPlanner planner = new HepPlanner(program);
                planner.setRoot(node);
                optimizedNode = planner.findBestExp();
                break;
            case "Volcano" :
                VolcanoPlanner volcanoPlanner = (VolcanoPlanner) node.getCluster().getPlanner();
                volcanoPlanner.addRule(JoinPushThroughJoinRule.LEFT);
                volcanoPlanner.addRule(AggregateExtractProjectRule.SCAN);
                volcanoPlanner.addRule(LoptOptimizeJoinRule.Config.DEFAULT.toRule());

                RelTraitSet desired = node.getTraitSet().replace(EnumerableConvention.INSTANCE).simplify();
                volcanoPlanner.setRoot(volcanoPlanner.changeTraits(node, desired));
                optimizedNode = volcanoPlanner.findBestExp();
                break;
        }
//        HepProgram program = HepProgram.builder().build();
//        HepPlanner planner = new HepPlanner(program);
//        planner.setRoot(node);
//        RelNode optimizedNode = planner.findBestExp();}

//        VolcanoPlanner planner = (VolcanoPlanner) node.getCluster().getPlanner();
//        RelTraitSet desired = node.getTraitSet().replace(EnumerableConvention.INSTANCE).simplify();
//        planner.setRoot(planner.changeTraits(node, desired));
//        RelNode optimizedNode = planner.findBestExp();


        //System.out.println(RelOptUtil.toString(optimizedNode));
        final RelRunner runner = connection.unwrap(RelRunner.class);
        PreparedStatement ps = runner.prepareStatement(optimizedNode);
        ps.setFetchSize(10000);
        ps.setFetchDirection(ResultSet.FETCH_FORWARD);

        System.err.println("Prepare time: " +
                (System.currentTimeMillis() - start));
        System.err.println("Q: " +
                queryText);
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
                sb.append("\"");
                sb.append(resultSet.getString(i));
                if (i < columnCount)
                    sb.append("\"|");
                else
                    sb.append("\"");
            }
            //Thread.sleep(1000);
            sb.append("\n");
            if (sb.length()>2048)
            {
                //out.write(sb.toString()) ;
                sb.setLength(0);
            }
        }
        if (sb.length()>0)
        {
            //out.write(sb.toString());
        }
        out.flush();
        out.close();
        System.err.println("Write time: " +
                (System.currentTimeMillis() - start));
        //System.in.read();

    }

    private static Map<String, String> SplitArgs(String[] args) {
        Map<String, String> output = new HashMap<>();
        if(args != null && args.length > 0){
        if(args[0]!= null && args.length == 1) {
            String[] arguments = args[0].split(";");
            for (int i = 0; i < arguments.length; i++) {
                String arg = arguments[i];
                int separatorIndex = arg.indexOf('=');
                String name = arg.substring(0, separatorIndex);
                String value = "";
                if (arg.length() > separatorIndex) {
                    value = arg.substring(separatorIndex + 1);
                }
                output.put(name, value);
            }
            }
        }

        output = SetDefaults(output);
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
        if(!output.containsKey("Plan"))
        {
            output.put("Plan", "Hep");
        }
        if(!output.containsKey(("QueryNumber")))
        {
            output.put("QueryNumber", "0");
        }
        if(!output.containsKey("Query"))
        {
            if(output.containsKey("QueryNumber"))
            {
                output.put("Query", GetQuery(Integer.parseInt(output.get("QueryNumber"))));
            }
            else {
                output.put("Query", "SELECT * FROM s.nation");}
//            output.put("Query", "SELECT   L_RETURNFLAG,   L_LINESTATUS,   L_QUANTITY,   L_EXTENDEDPRICE,   L_DISCOUNT,   L_TAX  " +
//                    "FROM s.LINEITEM " +
//                    "WHERE   L_SHIPDATE <= DATE '1998-12-01' - INTERVAL '90' DAY");
            //output.put("Query", GetQuery(Integer.parseInt(output.get("QueryNumber"))));
        }
        if(!output.containsKey("DriverClassName"))
        {
            output.put("DriverClassName", "com.mysql.cj.jdbc.Driver");
        }

        return output;
    }

    private static String GetQuery(int queryNumber) {
        switch (queryNumber) {
//            case 0: return  "SELECT   L_RETURNFLAG,   L_LINESTATUS,   L_QUANTITY,   L_EXTENDEDPRICE,   L_DISCOUNT,   L_TAX  " +
//                    "FROM s.LINEITEM " +
//                    "WHERE   L_SHIPDATE <= DATE '1998-12-01' - INTERVAL '90' DAY";
            case 0: return "SELECT * FROM s.NATION";
            case 1:
                return "SELECT " +
                        " L_RETURNFLAG, " +
                        " L_LINESTATUS, " +
                        " SUM(L_QUANTITY) AS SUM_QTY, " +
                        " SUM(L_EXTENDEDPRICE) AS SUM_BASE_PRICE, " +
                        " SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS SUM_DISC_PRICE, " +
                        " SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT) * (1 + L_TAX)) AS SUM_CHARGE, " +
                        " AVG(L_QUANTITY) AS AVG_QTY, " +
                        " AVG(L_EXTENDEDPRICE) AS AVG_PRICE, " +
                        " AVG(L_DISCOUNT) AS AVG_DISC, " +
                        " COUNT(*) AS COUNT_ORDER " +
                        "FROM " +
                        " s.LINEITEM " +
                        "WHERE " +
                        " L_SHIPDATE <= DATE '1998-12-01' - INTERVAL '90' DAY " +
                        "GROUP BY " +
                        " L_RETURNFLAG, " +
                        " L_LINESTATUS " +
                        "ORDER BY " +
                        " L_RETURNFLAG, " +
                        " L_LINESTATUS ";
            case 2:
                return "SELECT " +
                        " S_ACCTBAL, " +
                        " S_NAME, " +
                        " N_NAME, " +
                        " P_PARTKEY, " +
                        " P_MFGR, " +
                        " S_ADDRESS, " +
                        " S_PHONE, " +
                        " S_COMMENT " +
                        "FROM " +
                        " s.PART, " +
                        " s.SUPPLIER, " +
                        " s.PARTSUPP, " +
                        " s.NATION, " +
                        " s.REGION " +
                        "WHERE " +
                        " P_PARTKEY = PS_PARTKEY " +
                        " AND S_SUPPKEY = PS_SUPPKEY " +
                        " AND P_SIZE = 48 " +
                        " AND P_TYPE LIKE '%NICKEL' " +
                        " AND S_NATIONKEY = N_NATIONKEY " +
                        " AND N_REGIONKEY = R_REGIONKEY " +
                        " AND R_NAME = 'AMERICA' " +
                        " AND PS_SUPPLYCOST = ( " +
                        "  SELECT " +
                        "   MIN(PS_SUPPLYCOST) " +
                        "  FROM " +
                        "   s.PARTSUPP, " +
                        "   s.SUPPLIER, " +
                        "   s.NATION, " +
                        "   s.REGION " +
                        "  WHERE " +
                        "   P_PARTKEY = PS_PARTKEY " +
                        "   AND S_SUPPKEY = PS_SUPPKEY " +
                        "   AND S_NATIONKEY = N_NATIONKEY " +
                        "   AND N_REGIONKEY = R_REGIONKEY " +
                        "   AND R_NAME = 'AMERICA' " +
                        " ) " +
                        "ORDER BY " +
                        " S_ACCTBAL DESC, " +
                        " N_NAME, " +
                        " S_NAME, " +
                        " P_PARTKEY ";
            case 3 : return "SELECT " +
                    " L_ORDERKEY, " +
                    " SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE, " +
                    " O_ORDERDATE, " +
                    " O_SHIPPRIORITY " +
                    "FROM " +
                    " s.CUSTOMER, " +
                    " s.ORDERS, " +
                    " s.LINEITEM " +
                    "WHERE " +
                    " C_MKTSEGMENT = 'HOUSEHOLD' " +
                    " AND C_CUSTKEY = O_CUSTKEY " +
                    " AND L_ORDERKEY = O_ORDERKEY " +
                    " AND O_ORDERDATE < '1995-03-31' " +
                    " AND L_SHIPDATE > '1995-03-31' " +
                    "GROUP BY " +
                    " L_ORDERKEY, " +
                    " O_ORDERDATE, " +
                    " O_SHIPPRIORITY " +
                    "ORDER BY " +
                    " REVENUE DESC, " +
                    " O_ORDERDATE ";
            case 31 : return "SELECT " +
                    " L_ORDERKEY, " +
                    " O_ORDERDATE, " +
                    " O_SHIPPRIORITY " +
                    "FROM " +
                    " s.CUSTOMER, " +
                    " s.ORDERS, " +
                    " s.LINEITEM " +
                    "WHERE " +
                    " C_MKTSEGMENT = 'HOUSEHOLD' " +
                    " AND C_CUSTKEY = O_CUSTKEY " +
                    " AND L_ORDERKEY = O_ORDERKEY " +
                    " AND O_ORDERDATE < '1995-03-31' " +
                    " AND L_SHIPDATE > '1995-03-31' " +
                    "GROUP BY " +
                    " L_ORDERKEY, " +
                    " O_ORDERDATE, " +
                    " O_SHIPPRIORITY " +
                    "ORDER BY " +
                    " O_ORDERDATE ";
            case 4 : return "SELECT  " +
                    " O_ORDERPRIORITY, " +
                    "   COUNT(O_ORDERPRIORITY) AS ORDER_COUNT " +
                    "FROM  " +
                    "   ( " +
                    "SELECT " +
                    " L_ORDERKEY, " +
                    " O_ORDERPRIORITY " +
                    "FROM " +
                    " s.ORDERS, " +
                    " s.LINEITEM " +
                    "WHERE " +
                    " L_ORDERKEY = O_ORDERKEY " +
                    " AND L_COMMITDATE < L_RECEIPTDATE " +
                    " AND O_ORDERDATE >= date '1996-02-01' " +
                    " AND O_ORDERDATE < date '1996-02-01' + INTERVAL '3' MONTH " +
                    "  " +
                    "GROUP BY " +
                    " L_ORDERKEY " +
                    " ) T " +
                    "GROUP BY " +
                    " O_ORDERPRIORITY " +
                    "ORDER BY " +
                    " O_ORDERPRIORITY";
            case 5: return "SELECT " +
                    " N_NAME, " +
                    " SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE " +
                    "FROM " +
                    " s.CUSTOMER, " +
                    " s.ORDERS, " +
                    " s.LINEITEM, " +
                    " s.SUPPLIER, " +
                    " s.NATION, " +
                    " s.REGION " +
                    "WHERE " +
                    " C_CUSTKEY = O_CUSTKEY " +
                    " AND L_ORDERKEY = O_ORDERKEY " +
                    " AND L_SUPPKEY = S_SUPPKEY " +
                    " AND C_NATIONKEY = S_NATIONKEY " +
                    " AND S_NATIONKEY = N_NATIONKEY " +
                    " AND N_REGIONKEY = R_REGIONKEY " +
                    " AND R_NAME = 'MIDDLE EAST' " +
                    " AND O_ORDERDATE >= date '1995-01-01' " +
                    " AND O_ORDERDATE < date '1995-01-01' + INTERVAL '1' YEAR " +
                    "GROUP BY " +
                    " N_NAME " +
                    "ORDER BY " +
                    " REVENUE DESC ";
            case 6: return "SELECT " +
                    " SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS REVENUE " +
                    "FROM " +
                    " s.LINEITEM " +
                    "WHERE " +
                    " L_SHIPDATE >= '1997-01-01' " +
                    " AND L_SHIPDATE < '1997-01-01' + INTERVAL '1' YEAR " +
                    " AND L_DISCOUNT BETWEEN 0.07 - 0.01 AND 0.07 + 0.01 " +
                    " AND L_QUANTITY < 24 ";
            case 7: return "SELECT " +
                    " SUPP_NATION, " +
                    " CUST_NATION, " +
                    " L_YEAR, " +
                    " SUM(VOLUME) AS REVENUE " +
                    "FROM " +
                    " ( " +
                    "  SELECT " +
                    "   N1.N_NAME AS SUPP_NATION, " +
                    "   N2.N_NAME AS CUST_NATION, " +
                    "   EXTRACT(YEAR FROM L_SHIPDATE) AS L_YEAR, " +
                    "   L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS VOLUME " +
                    "  FROM " +
                    "   s.SUPPLIER, " +
                    "   s.LINEITEM, " +
                    "   s.ORDERS, " +
                    "   s.CUSTOMER, " +
                    "   s.NATION N1, " +
                    "   s.NATION N2 " +
                    "  WHERE " +
                    "   S_SUPPKEY = L_SUPPKEY " +
                    "   AND O_ORDERKEY = L_ORDERKEY " +
                    "   AND C_CUSTKEY = O_CUSTKEY " +
                    "   AND S_NATIONKEY = N1.N_NATIONKEY " +
                    "   AND C_NATIONKEY = N2.N_NATIONKEY " +
                    "   AND ( " +
                    "    (N1.N_NAME = 'IRAQ' AND N2.N_NAME = 'ALGERIA') " +
                    "    OR (N1.N_NAME = 'ALGERIA' AND N2.N_NAME = 'IRAQ') " +
                    "   ) " +
                    "   AND L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31' " +
                    " ) AS SHIPPING " +
                    "GROUP BY " +
                    " SUPP_NATION, " +
                    " CUST_NATION, " +
                    " L_YEAR " +
                    "ORDER BY " +
                    " SUPP_NATION, " +
                    " CUST_NATION, " +
                    " L_YEAR ";
            case 8: return "SELECT " +
                    " O_YEAR, " +
                    " SUM(CASE " +
                    "  WHEN NATION = 'IRAN' THEN VOLUME " +
                    "  ELSE 0 " +
                    " END) / SUM(VOLUME) AS MKT_SHARE " +
                    "FROM " +
                    " ( " +
                    "  SELECT " +
                    "   EXTRACT(YEAR FROM O_ORDERDATE) AS O_YEAR, " +
                    "   L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS VOLUME, " +
                    "   N2.N_NAME AS NATION " +
                    "  FROM " +
                    "   s.PART, " +
                    "   s.SUPPLIER, " +
                    "   s.LINEITEM, " +
                    "   s.ORDERS, " +
                    "   s.CUSTOMER, " +
                    "   s.NATION N1, " +
                    "   s.NATION N2, " +
                    "   s.REGION " +
                    "  WHERE " +
                    "   P_PARTKEY = L_PARTKEY " +
                    "   AND S_SUPPKEY = L_SUPPKEY " +
                    "   AND L_ORDERKEY = O_ORDERKEY " +
                    "   AND O_CUSTKEY = C_CUSTKEY " +
                    "   AND C_NATIONKEY = N1.N_NATIONKEY " +
                    "   AND N1.N_REGIONKEY = R_REGIONKEY " +
                    "   AND R_NAME = 'MIDDLE EAST' " +
                    "   AND S_NATIONKEY = N2.N_NATIONKEY " +
                    "   AND O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31' " +
                    "   AND P_TYPE = 'STANDARD BRUSHED BRASS' " +
                    " ) AS ALL_NATIONS " +
                    "GROUP BY " +
                    " O_YEAR " +
                    "ORDER BY " +
                    " O_YEAR ";
            case 9: return "SELECT " +
                    " NATION, " +
                    " O_YEAR, " +
                    " SUM(AMOUNT) AS SUM_PROFIT " +
                    "FROM " +
                    " ( " +
                    "  SELECT " +
                    "   N_NAME AS NATION, " +
                    "   EXTRACT(YEAR FROM O_ORDERDATE) AS O_YEAR, " +
                    "   L_EXTENDEDPRICE * (1 - L_DISCOUNT) - PS_SUPPLYCOST * L_QUANTITY AS AMOUNT " +
                    "  FROM " +
                    "   s.PART, " +
                    "   s.SUPPLIER, " +
                    "   s.LINEITEM, " +
                    "   s.PARTSUPP, " +
                    "   s.ORDERS, " +
                    "   s.NATION " +
                    "  WHERE " +
                    "   S_SUPPKEY = L_SUPPKEY " +
                    "   AND PS_SUPPKEY = L_SUPPKEY " +
                    "   AND PS_PARTKEY = L_PARTKEY " +
                    "   AND P_PARTKEY = L_PARTKEY " +
                    "   AND O_ORDERKEY = L_ORDERKEY " +
                    "   AND S_NATIONKEY = N_NATIONKEY " +
                    "   AND P_NAME LIKE '%SNOW%' " +
                    " ) AS PROFIT " +
                    "GROUP BY " +
                    " NATION, " +
                    " O_YEAR " +
                    "ORDER BY " +
                    " NATION, " +
                    " O_YEAR DESC ";
            case 91: return "SELECT " +
                    " NATION, " +
                    " O_YEAR, " +
                    " SUM(AMOUNT) AS SUM_PROFIT " +
                    "FROM " +
                    " ( " +
                    "  SELECT " +
                    "   N_NAME AS NATION, " +
                    "   EXTRACT(YEAR FROM O_ORDERDATE) AS O_YEAR, " +
                    "   L_EXTENDEDPRICE * (1 - L_DISCOUNT) - PS_SUPPLYCOST * L_QUANTITY AS AMOUNT " +
                    "  FROM " +
                    "   s.PART, " +
                    "   s.SUPPLIER, " +
                    "   s.LINEITEM, " +
                    "   s.PARTSUPP, " +
                    "   s.ORDERS, " +
                    "   s.NATION " +
                    "  WHERE " +
                    "   S_SUPPKEY = L_SUPPKEY " +
                    "   AND PS_SUPPKEY = L_SUPPKEY " +
                    "   AND PS_PARTKEY = L_PARTKEY " +
                    "   AND P_PARTKEY = L_PARTKEY " +
                    "   AND O_ORDERKEY = L_ORDERKEY " +
                    "   AND S_NATIONKEY = N_NATIONKEY " +
                    "   AND P_NAME LIKE '%SNOW%' " +
                    " ) AS PROFIT " +
                    "GROUP BY " +
                    " NATION, " +
                    " O_YEAR " +
                    "ORDER BY " +
                    " NATION, " +
                    " O_YEAR DESC ";
            case 10: return "SELECT " +
                    " C_CUSTKEY, " +
                    " C_NAME, " +
                    " SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE, " +
                    " C_ACCTBAL, " +
                    " N_NAME, " +
                    " C_ADDRESS, " +
                    " C_PHONE, " +
                    " C_COMMENT " +
                    "FROM " +
                    " s.CUSTOMER, " +
                    " s.ORDERS, " +
                    " s.LINEITEM, " +
                    " s.NATION " +
                    "WHERE " +
                    " C_CUSTKEY = O_CUSTKEY " +
                    " AND L_ORDERKEY = O_ORDERKEY " +
                    " AND O_ORDERDATE >= '1994-04-01' " +
                    " AND O_ORDERDATE < '1994-04-01' + INTERVAL '3' MONTH " +
                    " AND L_RETURNFLAG = 'R' " +
                    " AND C_NATIONKEY = N_NATIONKEY " +
                    "GROUP BY " +
                    " C_CUSTKEY, " +
                    " C_NAME, " +
                    " C_ACCTBAL, " +
                    " C_PHONE, " +
                    " N_NAME, " +
                    " C_ADDRESS, " +
                    " C_COMMENT " +
                    "ORDER BY " +
                    " REVENUE DESC ";
            case 11: return "SELECT " +
                    " PS_PARTKEY, " +
                    " SUM(PS_SUPPLYCOST * PS_AVAILQTY) AS VALUE " +
                    "FROM " +
                    " s.PARTSUPP, " +
                    " s.SUPPLIER, " +
                    " s.NATION " +
                    "WHERE " +
                    " PS_SUPPKEY = S_SUPPKEY " +
                    " AND S_NATIONKEY = N_NATIONKEY " +
                    " AND N_NAME = 'ALGERIA' " +
                    "GROUP BY " +
                    " PS_PARTKEY HAVING " +
                    "  SUM(PS_SUPPLYCOST * PS_AVAILQTY) > ( " +
                    "   SELECT " +
                    "    SUM(PS_SUPPLYCOST * PS_AVAILQTY) * 0.0001000000 " +
                    "   FROM " +
                    "    s.PARTSUPP, " +
                    "    s.SUPPLIER, " +
                    "    s.NATION " +
                    "   WHERE " +
                    "    PS_SUPPKEY = S_SUPPKEY " +
                    "    AND S_NATIONKEY = N_NATIONKEY " +
                    "    AND N_NAME = 'ALGERIA' " +
                    "  ) " +
                    "ORDER BY " +
                    " VALUE DESC ";
            case 12: return "SELECT " +
                    " L_SHIPMODE, " +
                    " SUM(CASE " +
                    "  WHEN O_ORDERPRIORITY = '1-URGENT' " +
                    "   OR O_ORDERPRIORITY = '2-HIGH' " +
                    "   THEN 1 " +
                    "  ELSE 0 " +
                    " END) AS HIGH_LINE_COUNT, " +
                    " SUM(CASE " +
                    "  WHEN O_ORDERPRIORITY <> '1-URGENT' " +
                    "   AND O_ORDERPRIORITY <> '2-HIGH' " +
                    "   THEN 1 " +
                    "  ELSE 0 " +
                    " END) AS LOW_LINE_COUNT " +
                    "FROM " +
                    " s.ORDERS, " +
                    " s.LINEITEM " +
                    "WHERE " +
                    " O_ORDERKEY = L_ORDERKEY " +
                    " AND L_SHIPMODE IN ('AIR', 'SHIP') " +
                    " AND L_COMMITDATE < L_RECEIPTDATE " +
                    " AND L_SHIPDATE < L_COMMITDATE " +
                    " AND L_RECEIPTDATE >= '1994-01-01' " +
                    " AND L_RECEIPTDATE < '1994-01-01' + INTERVAL '1' YEAR " +
                    "GROUP BY " +
                    " L_SHIPMODE " +
                    "ORDER BY " +
                    " L_SHIPMODE ";
            case 13: return "SELECT " +
                    " C_COUNT, " +
                    " COUNT(*) AS CUSTDIST " +
                    "FROM " +
                    " ( " +
                    "  SELECT " +
                    "   C_CUSTKEY, " +
                    "   COUNT(O_ORDERKEY) AS C_COUNT " +
                    "  FROM " +
                    "   s.CUSTOMER LEFT OUTER JOIN s.ORDERS ON " +
                    "    C_CUSTKEY = O_CUSTKEY " +
                    "    AND O_COMMENT NOT LIKE '%SPECIAL%REQUESTS%' " +
                    "  GROUP BY " +
                    "   C_CUSTKEY " +
                    " ) AS C_ORDERS " +
                    "GROUP BY " +
                    " C_COUNT " +
                    "ORDER BY " +
                    " CUSTDIST DESC, " +
                    " C_COUNT DESC ";
            case 14: return "SELECT " +
                    " 100.00 * SUM(CASE " +
                    "  WHEN P_TYPE LIKE 'PROMO%' " +
                    "   THEN L_EXTENDEDPRICE * (1 - L_DISCOUNT) " +
                    "  ELSE 0 " +
                    " END) / SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS PROMO_REVENUE " +
                    "FROM " +
                    " s.LINEITEM, " +
                    " s.PART " +
                    "WHERE " +
                    " L_PARTKEY = P_PARTKEY " +
                    " AND L_SHIPDATE >= '1995-01-01' " +
                    " AND L_SHIPDATE < '1995-01-01' + INTERVAL '1' MONTH ";
            default:
                return "SELECT * FROM s.NATION";
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
