package helpers


import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.client.ClientProtocolException
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient
import scala.util.parsing.json._
import net.liftweb.json._
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.File;
import java.io.PrintWriter;


class apiConnect(db: String) {
    // Searches NewsAPI all news category
    def searchEverything(search: String, sources: String, from: String, to: String, language: String, sortBy: String, page: String): Unit = {
        val url = "https://newsapi.org/v2/everything" + 
                  "?q=" + search +  
                  "&sources=" + sources +
                  "&from=" + from +
                  "&to=" + to +
                  "&language=" + language +
                  "&sortBy=" + sortBy +
                  "&pageSize=100" +
                  "&page=" + page +
                  "&apiKey=0425fffcba054e00a1e722ffb03f7ff5"
        val result = scala.io.Source.fromURL(url).mkString
        createFile(cleanJson(result), "allnews.csv")
        val hg = new hiveGo2(db)
        hg.populateTable("allnews")
    }
    // Searches NewsAPI all top news category with search param and category
    def searchTopHeadlines(search: String, country: String, category: String, sources: String, page: String): Unit = {
        val url = "https://newsapi.org/v2/top-headlines" + 
                  "?q=" + search +  
                  "&country=" + country +
                  "&category=" + category +
                  "&sources=" + sources +
                  "&pageSize=100" +
                  "&page=" + page +
                  "&apiKey=0425fffcba054e00a1e722ffb03f7ff5"
        val result = scala.io.Source.fromURL(url).mkString
        createFile(cleanJson(result), "topnews.csv")
        val hg = new hiveGo2(db)
        hg.populateTable("topnews")
    }
    // Searches NewsAPI all top news category without search param and category
    def searchTopHeadlinesAll(category: String, country: String, sources: String, page: String): Unit = {
        val url = "https://newsapi.org/v2/top-headlines" + 
                  "?q=" +
                  "&country=" + country +
                  "&category=" + category +
                  "&sources=" + sources +
                  "&pageSize=100" +
                  "&page=" + page +
                  "&apiKey=0425fffcba054e00a1e722ffb03f7ff5"
        val result = scala.io.Source.fromURL(url).mkString
        createFile(cleanJson(result), "topnewsall.csv")
        val hg = new hiveGo2(db)
        hg.populateTable("topnewsall")
    }
    // Cleans the input string of JSON to output in csv format
    def cleanJson(jString: String): String = {
        implicit val formats = net.liftweb.json.DefaultFormats
        var q = new StringBuilder("")
        val jValue = parse(jString)
        val resultDoc = jValue.extract[Default]
        for(article <- resultDoc.articles){
            val art = article.extract[AllFields]
            val r = art.source.extract[NewsSource]
            q = (q ++= s"${r.name}|${art.author}|${art.title}|${replace(art.description)}|${art.url}|${resultDoc.totalResults}\n")
        }
        q.toString()
    }
    // Helper function for cleanJson to replace \n \t \r if it is not null
    def replace(s: String): String = {
        try {
            s.replace("\n", " ").replace("\t"," ").replace("\r"," ")
        } catch {
            case e: Exception => s
        }
    }
    // Creates a CSV file with input string and name
    def createFile(result: String, name: String): Unit = {
        val path = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/project1/"
        val filename = path + name
        println(s"Creating file $filename ...")
        
        val conf = new Configuration()
        val fs = FileSystem.get(conf)
        
        // Check if file exists. If yes, delete it.
        println("Checking if it already exists...")
        val filepath = new Path( filename)
        val isExisting = fs.exists(filepath)
        if(isExisting) {
            println("Yes it does exist. Deleting it...")
            fs.delete(filepath, false)
        }

        val output = fs.create(new Path(filename))  
        val writer = new PrintWriter(output)
        writer.write(result)
        writer.close()
        
        println(s"Done creating file $filename ...\n")
    }
    // Helper class to drop, create and populate table from CSV file
    class hiveGo2(db: String) extends hiveGo() {
        def populateTable(tableName: String){
            try {
                println(s"\nDatabase: $db")
                if(!db.isEmpty){
                    val filepath = "/user/maria_dev/project1/"
                    val columns = "(name string, author string, title string, description string, url string,total int)"
                    println(s"Dropping table if exist...")
                    stmt.execute("DROP TABLE IF EXISTS " + tableName)
                    println("Loading file into table")
                    stmt.execute(
                        "create external table IF NOT EXISTS " + db + "."+ tableName + columns + " row format delimited fields terminated by '|' location '" + filepath + "'"
                    );
                    println("Table populated successfully\n")
                }else{
                    println("Select a database to use!")
                }
            }catch {
                case e: Throwable  => println(s"\n$e")
            }
        }
    }
}
// Case classes used to extract values from json string
case class Default(status: String, totalResults: Int, articles: List[net.liftweb.json.JObject])
case class AllFields(source: net.liftweb.json.JValue, author: String, title: String, description: String, url: String, urlToImage: String, publishedAt: String, content: String)
case class NewsSource(id: String, name: String)

