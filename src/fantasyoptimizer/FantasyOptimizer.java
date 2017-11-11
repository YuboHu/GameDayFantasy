package fantasyoptimizer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import java.math.BigDecimal;
import java.net.HttpURLConnection;

import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import database.Database;

public class FantasyOptimizer
{

//    private static final String m_dir = "D:/";// System.getProperty("user.dir");
//    private static final String m_allPath = m_dir + "/all.json";
//    private static final String m_rankPath = m_dir + "/rank.json";
//    private static final String m_lineupPath = m_dir + "/lineup.json";
//    private static final String m_140lineupPath = m_dir + "/140.json";
//    private static final String m_100lineupPath = m_dir + "/100.json";
//    private static final String m_80lineupPath = m_dir + "/80.json";
//    private static final String m_pkPath = m_dir + "/pk.json";

    private static final String m_listUrl = "http://rotoguru1.com/cgi-bin/nba-dhd-2018.pl";
    private static String m_method = "GET";
    private Map<String,String> todaysGame = new HashMap<String,String>();
    private static final double HomeAwayFactor = 0.9;
    private static final double Last3Factor = 0.7;
    private static final int minSalary = 3000;
    private Database m_database;

    public FantasyOptimizer()
    {
        m_database = new Database();
    }

    public static void main(String[] in_args) throws Exception
    {
        FantasyOptimizer fo = new FantasyOptimizer();
        //fo.updatePerformance();
        //fo.generatePrediction();
        //fo.updateSalary();
        //fo.generate9Lineup();
        fo.generate6Lineup();
    }

    private void generate6Lineup() throws Exception
    {
        m_database.connect();
        int baseline = 160;
        int maxSalary = 40000;
        String insert = "insert into 6Lineups values (?,?,?,?,?,?,?)";
        PreparedStatement statement = m_database.prepareStatement(insert);
        int budget = maxSalary - 6 * minSalary;
        ResultSet pg = getPlayer(budget,1);
        while(pg.next())
        {
            String pgName = pg.getString(1);
            Double pgPts = pg.getDouble(2);
            int pgCost = pg.getInt(3) - minSalary;
            ResultSet sg = getPlayer(budget - pgCost,2);
            while(sg.next())
            {
                String sgName = sg.getString(1);
                Double sgPts = sg.getDouble(2);
                int sgCost = sg.getInt(3) - minSalary;
                if(sgName.equals(pgName))
                {
                    continue;
                }
                ResultSet sf = getPlayer(budget-pgCost-sgCost,3);
                while(sf.next())
                {
                    String sfName = sf.getString(1);
                    Double sfPts = sf.getDouble(2);
                    int sfCost = sf.getInt(3) - minSalary;
                    if(sgName.equals(sfName))
                    {
                        continue;
                    }
                    ResultSet pf = getPlayer(budget-pgCost-sgCost-sfCost,4);
                    while(pf.next())
                    {
                        String pfName = pf.getString(1);
                        Double pfPts = pf.getDouble(2);
                        int pfCost = pf.getInt(3) - minSalary;
                        if(pfName.equals(sfName))
                        {
                            continue;
                        }
                        ResultSet c = getPlayer(budget-pgCost-sgCost-sfCost-pfCost,5);
                        while(c.next())
                        {
                            String cName = c.getString(1);
                            Double cPts = c.getDouble(2);
                            int cCost = c.getInt(3) - minSalary;
                            if(pfName.equals(cName))
                            {
                                continue;
                            }
                            ResultSet u = getPlayer(budget-pgCost-sgCost-sfCost-pfCost-cCost,0); 
                            while(u.next())
                            {
                                String uName = u.getString(1);
                                Double uPts = u.getDouble(2);
                                if(uName.equals(pgName)||uName.equals(sgName)||uName.equals(sfName)||uName.equals(pfName)||uName.equals(cName))
                                {
                                    continue;
                                }
                                Double totalpts = pgPts+sgPts+sfPts+pfPts+cPts+uPts;       
                                if(totalpts>baseline)
                                {
                                    statement.setString(1, pgName);
                                    statement.setString(2, sgName);
                                    statement.setString(3, sfName);
                                    statement.setString(4, pfName);
                                    statement.setString(5, cName);
                                    statement.setString(6, uName);
                                    statement.setDouble(7, totalpts);
                                    statement.addBatch();
                                }
                            }
                        }
                    }
                }
            }
        }
        statement.executeBatch();
        statement.close();
        m_database.disconnect();
        
    }


    private void generate9Lineup()
    {
        int maxSalary = 60000;
        
    }
    
    private ResultSet getPlayer(int budget, int i) throws SQLException
    {
        String query = "select Name,Prediction,GDSalary from predictions where GDSalary <= "+ (budget+3000);
        if(i>0)
        {
            query += " and pos = " + i;
        }
        ResultSet rs = m_database.execQuery(query);
        return rs;
    }


    private void updateSalary() throws Exception
    {
        m_database.connect();
        String update = "update predictions set GDSalary = ? where Name = ?";
        PreparedStatement statement = m_database.prepareStatement(update);
        String file = "D:/Workspace2/FantasyOptimizer/FantasyServer/salary/salary.csv";
        readFile(file,statement);  
        statement.executeBatch();
        statement.close();
        m_database.disconnect();
    }

    private void generatePrediction() throws Exception
    {
        m_database.connect();
        String insert = "insert ignore into predictions(PID,Pos,Name,Prediction,FDSalary,DKSalary) values (?,?,?,?,?,?)";
        PreparedStatement statement = m_database.prepareStatement(insert);
        for (Map.Entry<String, String> entry : todaysGame.entrySet())
        {
            String home = entry.getKey();
            String away = entry.getValue();
            predictForTeam(home,away,true,statement);
            predictForTeam(away,home,false,statement);
        }
        statement.executeBatch();
        statement.close();
        m_database.disconnect();
    }

    private void predictForTeam(String team, String oppo, boolean home,PreparedStatement statement) throws SQLException
    {
        List<Integer> players = getAllPlayer(team);
        for(Integer player : players)
        {
            ResultSet rs = m_database.execQuery("select Name, FDSalary, DKSalary,Pos from performance where PID = '" + player + "' order by Date desc limit 1");
            if(rs.next())
            {
                String name = rs.getString(1);
                int FDSalary = rs.getInt(2);
                int DKSalary = rs.getInt(3);
                int pos = rs.getInt(4);
                double prediction = getAvg(player);
                if(prediction > 5)
                {
                    prediction = Last3Factor * prediction + (1-Last3Factor) * getLast3(player);
                    prediction = HomeAwayFactor * prediction + (1-HomeAwayFactor) * getHome(player,home);
                    prediction += getOppoFactor(oppo, pos);
                }                
                statement.setInt(1, player);
                statement.setInt(2, pos);
                statement.setString(3, name);
                statement.setBigDecimal(4, new BigDecimal(prediction));
                statement.setInt(5, FDSalary);
                statement.setInt(6, DKSalary);
                statement.addBatch();    
            }
        }
        
    }

    private double getLast3(Integer player) throws SQLException
    {
        String query = "select avg(GameDayPoints) from performance as p1 join (select PID,GID from performance where PID = '" + player + "' order by Date desc limit 3) as p2 on p1.GID=p2.GID and p1.PID = p2.PID;";
        ResultSet rs = m_database.execQuery(query);
        if(rs.next())
        {
            return rs.getDouble(1);
        }
        return 0;
    }

    private double getOppoFactor(String oppo, int pos) throws SQLException
    {
        double improvement = 0;
        int player = 0;
        ResultSet rs = m_database.execQuery("select PID, avg(GameDayPoints) from performance where GP = true and Oppo = '" + oppo + "' and pos = " + pos + " group by PID");
        while(rs.next())
        {
            int pid = rs.getInt(1);
            double points = rs.getDouble(2);
            improvement += points - getAvg(pid);
            player ++;
        }
        
        return player == 0 ? 0:improvement/player;
    }

    private double getHome(Integer player, boolean home) throws SQLException
    {
        ResultSet rs = m_database.execQuery("select avg(GameDayPoints) from performance where GP = true and PID = " + player + " and Home = " + (home?"true":"false"));
        if(rs.next())
        {
            return rs.getDouble(1);
        }
        return 0;
    }

    private double getAvg(Integer player) throws SQLException
    {
        ResultSet rs = m_database.execQuery("select avg(GameDayPoints) from performance where GP = true and PID = " + player);
        if(rs.next())
        {
            return rs.getDouble(1);
        }
        return 0;
    }

    private List<Integer> getAllPlayer(String team) throws SQLException
    {
        List<Integer> players = new ArrayList<Integer>();
        ResultSet rs = m_database.execQuery("select distinct PID from performance where Team = '"+ team + "'");
        while(rs.next())
        {
            players.add(rs.getInt(1));
        }
        return players;
    }

    public void updatePerformance() throws Exception
    {
        todaysGame.clear();
        URL object = new URL(m_listUrl);

        HttpURLConnection con = (HttpURLConnection) object.openConnection();
        con.setRequestMethod(m_method);
        int HttpResult = con.getResponseCode();
        m_database.connect();
        String insert = "insert into performance values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement statement = m_database.prepareStatement(insert);
        if (HttpResult == HttpURLConnection.HTTP_OK)
        {
            BufferedReader br =
                new BufferedReader(new InputStreamReader(con.getInputStream(), "utf-8"));
            String line = br.readLine();
            while ((line = br.readLine()) != null && !line.isEmpty())
            {
                parsePerformance(line,statement);
            }
            statement.executeBatch();
            statement.close();
        }
        else
        {
            System.out.println(con.getResponseMessage());
        }
        m_database.disconnect();
    }

    private void parsePerformance(String line, PreparedStatement statement) throws SQLException
    {
        String[] data = line.split(Pattern.quote(":"));
        String PId = data[0];
        String GId = data[7];
        if(data.length > 9 && data[9].isEmpty())
        {
            String hometeam = data[4];
            String awayteam = data[5];
            if(data[6].equals("A"))
            {
                hometeam = data[5];
                awayteam = data[4];
            }
            todaysGame.put(hometeam, awayteam);
        }
        if(!checkGIdExists(GId,PId)&&data.length>30&&!data[9].isEmpty())
        {
            int pid = Integer.valueOf(data[0]);
            String name = data[2];
            String date = data[3];
            String team = data[4];
            String oppo = data[5];
            boolean home = data[6].equals("H")? true:false;
            boolean start = data[11].equals("1")? true:false;
            BigDecimal min = new BigDecimal(data[12]);
            boolean gp = data[13].equals("1")? true:false;
            BigDecimal GDpoints = countGDpoints(data[19]);
            int FDSalary = data[22].isEmpty()? 0:Integer.valueOf(data[22]);
            int DKSalary = data[24].isEmpty()? 0:Integer.valueOf(data[24].replaceAll("\\s",""));
            int pos = data[30].isEmpty()?(data.length > 33?(data[33].isEmpty()?0:Integer.valueOf(data[33])):0):Integer.valueOf(data[30]);
            statement.setInt(1, pid);
            statement.setString(2, name);
            statement.setString(3, date);
            statement.setString(4, team);
            statement.setString(5, oppo);
            statement.setBoolean(6, home);
            statement.setString(7, GId);
            statement.setBoolean(8, start);
            statement.setBigDecimal(9, min);
            statement.setBoolean(10, gp);
            statement.setBigDecimal(11, GDpoints);
            statement.setInt(12, FDSalary);
            statement.setInt(13, DKSalary);
            statement.setInt(14, pos);
            statement.addBatch();
        }

    }

    private BigDecimal countGDpoints(String string)
    {
        double points = 0;
        points += getNum("pt",string) + getNum("rb",string) * 1.2 + getNum("as",string) * 1.5 
            + getNum("st",string) * 2 + getNum("bl",string) * 2 - getNum("to",string);
        return BigDecimal.valueOf(points);
    }

    private int getNum(String suffix, String string)
    {
        int index = string.lastIndexOf(suffix);
        if(index > 0)
        {
            int index2 = string.substring(0, index).lastIndexOf(" ") + 1;
            return Integer.valueOf(string.substring(index2, index));
        }
        return 0;
    }


    private boolean checkGIdExists(String gId, String pId) throws SQLException
    {
        ResultSet rs = m_database.execQuery("select * from performance where GID = '" + gId + "' and PID = '" + pId + "'");
        return rs.next();
    }

//    private void generateLineupJson(String outputPath, ArrayList<Lineup> lineups) throws Exception
//    {
//        int bound = lineups.size() > 20 ? 20 : lineups.size();
//
//        JsonFactory factory = new JsonFactory();
//        JsonGenerator generator = factory.createGenerator(new File(outputPath), JsonEncoding.UTF8);
//
//        // Write the topmost '{'
//        generator.writeStartObject();
//
//        generator.writeFieldName("lineup");
//        generator.writeStartArray();
//
//        for (int l = 0; l < bound; l++)
//        {
//            Lineup lineup = lineups.get(l);
//            generator.writeStartObject();
//
//            Player pg = lineup.m_pg;
//            generator.writeFieldName("pg");
//            outputPlayer(generator, pg);
//
//            Player sg = lineup.m_sg;
//            generator.writeFieldName("sg");
//            outputPlayer(generator, sg);
//
//            Player sf = lineup.m_sf;
//            generator.writeFieldName("sf");
//            outputPlayer(generator, sf);
//
//            Player pf = lineup.m_pf;
//            generator.writeFieldName("pf");
//            outputPlayer(generator, pf);
//
//            Player c = lineup.m_c;
//            generator.writeFieldName("c");
//            outputPlayer(generator, c);
//
//            generator.writeNumberField("total", lineup.score);
//
//            generator.writeEndObject();
//        }
//
//        generator.writeEndArray();
//        // Write the bottommost '}'
//        generator.writeEndObject();
//        generator.close();
//    }
//
//    private void outputPlayer(JsonGenerator generator, Player p) throws Exception
//    {
//        generator.writeStartObject();
//        generator.writeStringField("name", p.m_displayName);
//        generator.writeStringField("headImg", p.m_headImg);
//        generator.writeEndObject();
//    }
//
    private void readFile(String path, PreparedStatement statement) throws Exception
    {
        File yourFile = new File(path);
        if (!yourFile.exists())
        {
            throw new RuntimeException("Can find salary file");
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(
            new FileInputStream(path),
            "UTF8"));
        String line = null;

        while ((line = reader.readLine()) != null)
        {
            String[] data = line.split(",");
            statement.setInt(1, Integer.valueOf(data[1]));
            statement.setString(2, data[0]);
            statement.addBatch();
        }
        reader.close();
    }

}
