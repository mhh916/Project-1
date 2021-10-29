package helpers


import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.client.ClientProtocolException
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.File;
import java.io.PrintWriter;

class apiConnect() {
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
        createFile(result)
    }

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
        
        createFile(result)
    }

    def createFile(result: String): Unit = {
        val path = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/"
        val filename = path + "Output.json"
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
}