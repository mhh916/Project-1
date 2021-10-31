package helpers

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.PrintWriter;
import java.io.IOException
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;


import java.sql.Driver


class hiveConnect(user: String, password: String) {
    
    def authenticate(): auth = {
        val auth = new auth(true, true)
        
        return auth
    }

    def login(): Unit = {
        var loop = true
        try {
            if(authenticate().log){
                if(authenticate().admin){
                    admin()
                }else{
                    user()
                }
            }else{
                println("Wrong Password or Username\n")
            }
                
        }catch {
            case e: MatchError => println("Please pick a number between 1~8\n")
            case e: NumberFormatException => println("\nPlease enter a number\n") 
        }

    }

    def admin(): Unit = {
        var loop = true
        try {
            val hg = new hiveGo()
            var db: String = ""
            do {
                println()
                println("Please select an option")
                println("1. Pull Data\n2. Show Databases\n3. Use Database\n4. Create Table\n5. Show Tables\n6. Describe Table\n7. Load Data into Table\n8. Test\n9. Log Out")
                val option = scala.io.StdIn.readInt()
                println()
                        
                option match{
                    case 1 => {
                        hg.pullData(db)
                        

                    }
                    case 2 => { 
                        hg.showDB()
                        
                    }
                    case 3 => {
                        var loop3: Boolean = true
                        do{
                            db = hg.useDB()
                            if(hg.getDBList().contains(db)){
                                loop3 = false
                            }else{
                                println("Not a database")
                            }

                        }while(loop3)
                                 
                    }
                    case 4 => { 
                        hg.createTable(db)
                        // NEEDS WORK
                    }
                    case 5 => {
                        hg.showTables(db)     
                    } 
                    case 6 => {
                        hg.describeTable()            
                    } 
                    case 7 => {
                        hg.loadDataIntoTable()            
                    } 
                    case 8 => {
                                
                    }
                    case 9 => {
                        loop = false 
                        hg.closeConnection()           
                    }   
                }   
                
            }while(loop)
        }catch{
            case e: MatchError => println("Please pick a number between 1~4\n")
            case e: NumberFormatException => println("\nPlease enter a number\n") 
        }
    }    

    def user(): Unit = {
            var loop = true
            try {
                do {
                    println("Please select an option")
                    println("1. Top Keyword related to Gaming\n2. News source that talks about gaming the most\n3. Most popular console.\n4. Most popular game\n5. Most popular game publisher\n6. Percent of all top news headlines related to gaming\n7. Log Out")
                    val option = scala.io.StdIn.readInt()
                    println()
                            
                    option match{
                        case 1 => {
                            
                        }
                        case 2 => {
                                    
                        }
                        case 3 => {
                            loop = false          
                        }
                        case 4 => {
                                    
                        }  
                    }
                }while(loop)

            }catch{
                case e: MatchError => println("Please pick a number between 1~4\n")
                case e: NumberFormatException => println("\nPlease enter a number\n") 
            }
    }
}

case class auth(log: Boolean, admin: Boolean)

class hiveGo(){
    var con: java.sql.Connection = null
    var stmt: java.sql.Statement = null
    var dbList = List[String]()

    try {
    // For Hive2:
    var driverName = "org.apache.hive.jdbc.HiveDriver"
    val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/default";

    // For Hive1:
    // var driverName = "org.apache.hadoop.hive.jdbc.HiveDriver"
    // val conStr = "jdbc:hive://sandbox-hdp.hortonworks.com:10000/default";

    Class.forName(driverName)
    con = DriverManager.getConnection(conStr, "", "")
    stmt = con.createStatement()

    }catch{
        case ex: Throwable  => {
          ex.printStackTrace();
          throw new Exception(s"${ex.getMessage}")
        }
    }
    def getDBList(): List[String] = {
        return dbList
    }
    def useDB(): String = {
        var res = stmt.executeQuery("Show databases");
        while (res.next()) {
            if(!dbList.contains(res.getString(1))){
                dbList =  res.getString(1) :: dbList
            }
        }
        val database = scala.io.StdIn.readLine("Enter Database: ")     
        database
    }

    def showDB(): Unit = {
        var res = stmt.executeQuery("Show databases");
        while (res.next()) {
            println(s"${res.getString(1)}")
            if(!dbList.contains(res.getString(1))){
                dbList =  res.getString(1) :: dbList
            }
        }
        System.out.println("\nshow database successfully\n");
        println(dbList)
    }


    def select(column: String, table: String): Unit = {
      println(s"Executing SELECT $column FROM $table..")
      var res = stmt.executeQuery("SELECT " + column + " FROM " + table)
      while (res.next()) {
        println(s"${res.getString(1)}, ${res.getString(3)}")
        //System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2))
        //System.out.println(res.getString(1))
      }
    }
    //Create Exception handling for entering wrong column layout
    def createTable(db: String): Unit = {
        try {
            println(s"Database: $db")
            if(!db.isEmpty){
                val tableName = scala.io.StdIn.readLine("Enter Table Name: ") 
                val columns = scala.io.StdIn.readLine("Enter Column and Datatypes: ")
                println(s"Dropping table $tableName..")
                stmt.execute("drop table IF EXISTS " + tableName);
                println(s"Creating table $tableName..")
                stmt.execute("use " + db)
                stmt.execute(
                    "create table " + tableName + " " + columns +" row format delimited  fields terminated by ','"
                );
            }else{
                println("Select a database to use!")
            }
        }catch {
            case e: Throwable  => println("\nIncorrect syntax!!\nTry again\n")
        }
    }
    
    // Create exception handling for wrong db entered
    def showTables(db: String): Unit = {
        if(!db.isEmpty){
            // show tables
            println(s"Show TABLES In $db..")
            var sql = "show tables from " + db
            var res = stmt.executeQuery(sql)
            if (res.next()) {
                System.out.println(res.getString(1))
            }
        }else{
            println("Please use a database first!!")
        }
    }
    
    def describeTable(): Unit = {
        val tableName = scala.io.StdIn.readLine("Enter Table Name: ") 
        // describe table
        println(s"Describing table $tableName..")
        var sql = "describe " + tableName
        System.out.println("Running: " + sql)
        var res = stmt.executeQuery(sql)
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2))
        }
    }

    def loadDataIntoTable(): Unit = {
        val tableName = scala.io.StdIn.readLine("Enter Table Name: ")
        val filepath = "/tmp/a.txt";
        // load data into table
        // NOTE: filepath has to be local to the hive server
        // NOTE: /tmp/a.txt is a comma separated file with two fields per line
        // For E.g.; 1,"One"
        var sql = "load data local inpath '" + filepath + "' into table " + tableName
        System.out.println("Running: " + sql)
        stmt.execute(sql);
    }

    def closeConnection(): Unit = {
        try {
        if (con != null)
          con.close();
      } catch {
        case ex: Throwable  => {
          ex.printStackTrace();
          throw new Exception(s"${ex.getMessage}")
        }
      }
    }

    def pullData(db: String): Unit = {
        val api = new apiConnect(db)
        var loop = true
        try {
            do {
                println("Please select an option")
                println("1. Pull All News\n2. Pull Top Headlines\n3. Go Back")
                val option = scala.io.StdIn.readInt()
                println()
                           
                option match{
                    case 1 => {
                        api.searchEverything("game", "","" ,"","","", "1")
                    }
                    case 2 => {
                        api.searchTopHeadlines("game","","technology","","1")
                    }
                    case 3 => {
                        loop = false
                    }
                }



            }while(loop)
        }catch{
            case e: MatchError => println("Please pick a number between 1~2\n")
            case e: NumberFormatException => println("\nPlease enter a number\n") 
        }
    }
}
