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
    var db: String = ""
    
    // Calls loginHelper to get login info from DB and decrypts password and checks if logins match
    def authenticate(): Auth = {
        var auth = new Auth(false, false)
        val column = "username, password, admin"
        val table = "project1.notpasswords"
        val logins = hg.loginHelper(column,table)
        for (login <- logins){
            val username = login._1
            val pass = login._2
            val admin = login._3
            val cipher = new Cipher(pass)
            if(user == username && password == cipher.getDecryptedPassword()){
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

    // Login loops used to authenticate login information
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

    // Admin user Loop to run functions
    def admin(): Unit = {
        var loop = true
        try {
            
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
                }   
                
            }while(loop)
        }catch{
            case e: MatchError => println("Please pick a number between 1~8\n")
            case e: NumberFormatException => println("\nPlease enter a number\n") 
        }
    }    

    // Basic user Loop to run functions
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
                            hg.questionOne(db)
                        }
                        case 2 => {
                            hg.questionTwo(db)            
                        }
                        case 3 => {
                            hg.questionThree(db)             
                        }
                        case 4 => {
                            hg.questionFour(db)            
                        }
                        case 5 => {
                            hg.questionFive(db)            
                        }
                        case 6 => {
                            hg.questionSix(db)            
                        }
                        case 7 => {
                            loop = false
                            hg.closeConnection()             
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

// Helper hive class used to run basic queries
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
    // Returns the current list of stored Databases
    def getDBList(): List[String] = {
        return dbList
    }
    // Uses a DB from the list of stored Databases
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
    // Will show all current stored Databases
    def showDB(): Unit = {
        var res = stmt.executeQuery("Show databases");
        while (res.next()) {
            println(s"${res.getString(1)}")
            if(!dbList.contains(res.getString(1))){
                dbList =  res.getString(1) :: dbList
            }
        }
        println("\nshow database successfully\n");
        for(d <- dbList){
            println(d)
        }
    }
    // Creates a new user with provided info, encrypts password to store on hive
    def createUser(db: String): Unit = {
         try {
            if(!db.isEmpty){
                val tableName = "notpasswords"
                val username = scala.io.StdIn.readLine("Enter Username: ") 
                val password = scala.io.StdIn.readLine("Enter Password: ")
                val admin = scala.io.StdIn.readLine("Enter Admin Status: ")
                val cipher = new Cipher(password)
                println(s"Adding user to $tableName Table..")
                stmt.execute(
                    "insert into table project1.notpasswords select * from (select " + {'"'} + username + {'"'} + ", "+ {'"'} + cipher.getEncryptedPassword() + {'"'}  +", " + {'"'} + admin + {'"'} + ")a"
                );
                println("Success")
            }else{
                println("Select a database to use!")
            }
        }catch {
            case e: Throwable  => println("\nIncorrect syntax!!\nTry again\n")
        }    
    }

    // Helper function used to grab all login info from hive database as a ListBuffer
    def loginHelper(column: String, table: String): ListBuffer[(String, String, Boolean)] = {
      var logins = new ListBuffer[(String, String, Boolean)]()
      println("\nAuthenticating...")
      var res = stmt.executeQuery("SELECT " + column + " FROM " + table)
      while (res.next()) {
        logins += ((res.getString(1),res.getString(2), res.getString(3).toBoolean))
      }
      logins
    }
    // Creates a table with provided information from user in hive
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
    
    // Shows all tables from currently selected Database
    def showTables(db: String): Unit = {
        if(!db.isEmpty){
            // show tables
            println(s"Showing tables in $db..")
            var sql = "show tables from " + db
            var res = stmt.executeQuery(sql)
            while (res.next()) {
                println(res.getString(1))
            }
        }else{
            println("Please use a database first!!")
        }
    }
    // Describe a table by name input from current selected Database
    def describeTable(db: String): Unit = {
        val tableName = scala.io.StdIn.readLine("Enter Table Name: ") 
        // describe table
        println(s"Describing table $db.$tableName..")
        var sql = "describe " + db + "." + tableName
        var res = stmt.executeQuery(sql)
        while (res.next()) {
            println(res.getString(1) + "\t" + res.getString(2))
        }
    }
    // Closes the database connection
    def closeConnection(): Unit = {
        try {
        if (con != null)
          println("Closing connection...")
          con.close();
      } catch {
        case ex: Throwable  => {
          ex.printStackTrace();
          throw new Exception(s"${ex.getMessage}")
        }
      }
    }
    // Pulls the data from NewsAPI with provided search paramenters and stores it in Hive
    def pullData(db: String): Unit = {
        if(!db.isEmpty){
            val api = new apiConnect(db)
            var loop = true
            try {
                do {
                    println("Please select an option")
                    println("1. Pull All News\n2. Pull Gaming Top Headlines\n3. Pull Top Headlines\n4. Go Back")
                    val option = scala.io.StdIn.readInt()
                    println()
                            
                    option match{
                        case 1 => {
                            api.searchEverything("game", "","" ,"","en","", "1")
                        }
                        case 2 => {
                            api.searchTopHeadlines("game","us","technology","","1")
                        }
                        case 3 =>{
                            api.searchTopHeadlinesAll("technology","us","","1")
                        }
                        case 4 => {
                            loop = false
                        }
                    }
                }while(loop)
            }catch{
                case e: MatchError => println("Please pick a number between 1~4\n")
                case e: NumberFormatException => println("\nPlease enter a number\n") 
            }
        }else{
            println("Please use a database first!!")
        }
    }

    //1. Top Keyword related to Gaming
    def questionOne(db: String): Unit = {
        val bannedWords = List[String](" ","-","2","A","Atlanta","Houston","Korean","On","Series","Squid","against","an","announced",
                                      "available","back","been","but","can", "coming", "game.", "his", "how", "just", "like", "not",
                                       "or", "out", "over", "popular", "show", "some", "studio", "their", "two", "up", "was", "what", 
                                       "which", "working","a", "the", "word", "in", "of", "to", "The", "by", "for", "has", "is", "Game", 
                                       "game", "its", "on", "that", "with", "games", "it","will", "about", "are", "as", "at", "be", "de", 
                                       "from", "have", "more", "new", "one", "series", "video", "you", "and", "world","en", "la", "el", 
                                       "que", "un", "o", "der", "para", "y", "da", "na", "se", "com", "no", "do", "first", "this", "à", 
                                       "die", "por", "le", "avec", "les", "una", ",", "La", "World", "all", "des", "latest", "los", "since", 
                                       "et", "es", "Check", "em", "plus", "early", "Call", "con", "é", "…", "also", "after", "une", "we", 
                                       "November", "In", "look", "Pass", "War", "und", "game", "Le", "become", "iconic", "upcoming", "pour", "e", 
                                       "3", "your", "null", "sur", "team", "est", "he", "на", "now", "when", "—", "only", "made", "As", "they",
                                       "And","our","may","Animal","than")
        println("Running Query...\n")
        var res = stmt.executeQuery("select word, count(1) as cnt from ( select explode(split(description, ' ')) as word from demo.allnews) q  where word != " + {'"'} + " " + {'"'} + "group by word having count(1)>6 order by cnt desc")
        while (res.next()) {
            if(!bannedWords.contains(res.getString(1)) && !" ".contains(res.getString(1))){
                println(s"Top Keyword: ${res.getString(1)} Occurences: ${res.getString(2).toInt}")
            }
        }
        println()
    }
    //2. News source that talks about gaming the most
    def questionTwo(db: String): Unit = {
        println("Running Query...\n")
        var res = stmt.executeQuery("select name, count(*) as cnt from demo.topnews group by name order by cnt desc limit 1")
        while (res.next()) {
            println(s"News Source: ${res.getString(1)} Occurences: ${res.getString(2).toInt}\n")
        }
    }
    //3. Most popular console.
    def questionThree(db: String): Unit = {
        val consoles = List[String]("PlayStation", "PS5", "PS4", "PS3","XBOX","Xbox", "Nintendo","Switch")
        println("Running Query...\n")
        var res = stmt.executeQuery("select word, count(1) as cnt from ( select explode(split(description, ' ')) as word from demo.allnews) q  group by word having count(1)>1 order by cnt desc")
        while (res.next()) {
            if(consoles.contains(res.getString(1))){
                println(s"Console: ${res.getString(1)} Occurences: ${res.getString(2).toInt}")
            }
        }
        println()    
    }
    //4. Most popular game
    def questionFour(db: String): Unit = {
        val games = Map("League" -> "League of Legends", "Duty" -> "Call of Duty",
                         "Resident" -> "Resident Evil","Skater" -> "Pro Skater","PUBG" -> "PUBG", "Auto" -> "Grand Theft Auto","GTA" -> "Grand Theft Auto",
                         "Pokémon" -> "Pokémon", "Fantasy" -> "Final Fantasy", "Souls" -> "Dark Souls", "Forza" -> "Forza Horizon 5", "Pikmin" -> "Pikmin Bloom", "Splitgate" -> "Splitgate",
                         "Zoo" -> "Zoo simulator", "Genshin" -> "Genshin Impact", "Animal" -> "Animal Crossing")
        println("Running Query...\n")
        var res = stmt.executeQuery("select word, count(1) as cnt from ( select explode(split(description, ' ')) as word from demo.allnews) q group by word having count(1)>1 order by cnt desc")
        while (res.next()) {
            if(games.contains(res.getString(1))){
                println(s"Game: ${games(res.getString(1))} Occurences: ${res.getString(2).toInt}")
            }
        } 
        println()   
    }
    //5. Most popular game publisher
    def questionFive(db: String): Unit = {
        val publishers = List[String]("Nintendo", "EA", "Sony", "XSEED", "Capcom", "Activision", "Blizzard", "Ubisoft", "Sega", "Bethesda", "Digerati", "NIS", "Focus", "THQ", "Paradox","505","Aksys", "Microsoft", "Konami")
        println("Running Query...\n")
        var res = stmt.executeQuery("select word, count(1) as cnt from ( select explode(split(description, ' ')) as word from demo.allnews) q  group by word having count(1)>1 order by cnt desc")
        while (res.next()) {
            if(publishers.contains(res.getString(1))){
                println(s"Game Publisher: ${res.getString(1)} Occurences: ${res.getString(2).toInt}")
            }
        }
        println()      
    }
    //6. Percent of all top news headlines related to gaming
    def questionSix(db: String): Unit = {
        var game = new ListBuffer[(String)]()
        println("Running Query...\n")
        var res = stmt.executeQuery("select distinct total from demo.topnewsall")
        while (res.next()) {
            game += res.getString(1)
        }
        if(game(0) != null || !game(0).contains("null")){
            var t1: Double = game(0).toDouble
            var t2: Double = game(1).toDouble
            var percent = t1/t2 * 100
            println(f"Percentage: $percent%.2f %%\n")         
        }else {
            var t1: Double = game(1).toDouble
            var t2: Double = game(2).toDouble
            var percent = t1/t2 * 100
            println(f"Percentage: $percent%.2f %%\n")     
        }
        
    }
}
