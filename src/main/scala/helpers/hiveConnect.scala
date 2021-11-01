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
import scala.collection.mutable.ListBuffer


import java.sql.Driver


class hiveConnect(user: String, password: String) {
    val hg = new hiveGo()

    def authenticate(): Auth = {
        var auth = new Auth(false, false)
        val column = "username, password, admin"
        val table = "project1.notpasswords"
        val logins = hg.select(column,table)
        for (login <- logins){
            val username = login._1
            val pass = login._2
            val admin = login._3
            if(user == username && password == pass){
                if(admin){
                    auth = new Auth(true, true)
                    println(s"Successfully logged in as $username\n")
                }else{
                    auth = new Auth(true, false)
                    println(s"Successfully logged in as $username\n")
                } 
            }
        }
        return auth
    }

    def login(): Unit = {
        var loop = true
        try {
            val auth: Auth = authenticate()
            if(auth.log){
                if(auth.admin){
                    admin()
                }else{
                    user()
                }
            }else{
                println("Wrong Password or Username!!\n")
            }
                
        }catch {
            case e: MatchError => println("Please pick a number between 1~8\n")
            case e: NumberFormatException => println("\nPlease enter a number\n") 
        }

    }

    def admin(): Unit = {
        var loop = true
        try {
            var db: String = ""
            do {
                println()
                println("Please select an option")
                println("1. Pull & Load Data\n2. Show Databases\n3. Use Database\n4. Create Table\n5. Show Tables\n6. Describe Table\n7. Create User\n8. Log Out")
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
                    }
                    case 5 => {
                        hg.showTables(db)     
                    } 
                    case 6 => {
                        hg.describeTable(db)            
                    } 
                    case 7 => {
                        hg.createUser(db)            
                    } 
                    case 8 => {
                        loop = false 
                        hg.closeConnection()             
                    }
                    case 9 => {
                                 
                    }   
                }   
                
            }while(loop)
        }catch{
            case e: MatchError => println("Please pick a number between 1~8\n")
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
                                     
                        }
                        case 4 => {
                                    
                        }
                        case 5 => {
                                    
                        }
                        case 6 => {
                                    
                        }
                        case 7 => {
                            loop = false             
                        }    
                    }
                }while(loop)

            }catch{
                case e: MatchError => println("Please pick a number between 1~7\n")
                case e: NumberFormatException => println("\nPlease enter a number\n") 
            }
    }
}

case class Auth(log: Boolean, admin: Boolean)

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

    def createUser(db: String): Unit = {
         try {
            println(s"Database: $db")
            if(!db.isEmpty){
                val tableName = "notpasswords"
                val username = scala.io.StdIn.readLine("Enter Username: ") 
                val password = scala.io.StdIn.readLine("Enter Password: ")
                val admin = scala.io.StdIn.readLine("Enter Admin Status: ") 
                println(s"Adding user to $tableName Table..")
                stmt.execute(
                    "insert into table " + db + "." + tableName + " select * from (select " + {'"'} + username + {'"'} + ", "+ {'"'} + password + {'"'}  +", " + {'"'} + admin + {'"'} + ")a"
                );
            }else{
                println("Select a database to use!")
            }
        }catch {
            case e: Throwable  => println("\nIncorrect syntax!!\nTry again\n")
        }    
    }

    def select(column: String, table: String): ListBuffer[(String, String, Boolean)] = {
      var logins = new ListBuffer[(String, String, Boolean)]()
      println("\nAuthenticating...")
      var res = stmt.executeQuery("SELECT " + column + " FROM " + table)
      while (res.next()) {
        //println(s"${res.getString(1)},${res.getString(2)}, ${res.getString(3)}")
        logins += ((res.getString(1),res.getString(2), res.getString(3).toBoolean))
      }
      logins
    }

    def createTable(db: String): Unit = {
        try {
            println(s"Database: $db")
            if(!db.isEmpty){
                val tableName = scala.io.StdIn.readLine("Enter Table Name: ") 
                val columns = scala.io.StdIn.readLine("Enter Column and Datatypes: ")
                println(s"Dropping table $tableName..")
                stmt.execute("drop table IF EXISTS " + db + "." + tableName);
                println(s"Creating table $tableName..")
                stmt.execute(
                    "create table " + db + "." + tableName + " " + columns +" row format delimited  fields terminated by ','"
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
    
    def describeTable(db: String): Unit = {
        val tableName = scala.io.StdIn.readLine("Enter Table Name: ") 
        // describe table
        println(s"Describing table $db.$tableName..")
        var sql = "describe " + tableName
        System.out.println("Running: " + sql)
        var res = stmt.executeQuery(sql)
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2))
        }
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
        if(!db.isEmpty){
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
                case e: MatchError => println("Please pick a number between 1~3\n")
                case e: NumberFormatException => println("\nPlease enter a number\n") 
            }
        }else{
            println("Please use a database first!!")
        }
    }
}
