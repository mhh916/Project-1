package helpers

import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.client.ClientProtocolException
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient

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
        println(result)
    }

    def searchTopHeadlines(search: String, country: String, category: String, sources: String, page: String): Unit = {
        val url = "https://newsapi.org/v2/everything" + 
                  "?q=" + search +  
                  "&country=" + country +
                  "&category=" + category +
                  "&sources=" + sources +
                  "&pageSize=100" +
                  "&page=" + page +
                  "&apiKey=0425fffcba054e00a1e722ffb03f7ff5"
        val result = scala.io.Source.fromURL(url).mkString
        println(result)
    }


    def simpleApi(search: String, category: String): Unit = {
    val url = "https://newsapi.org/v2/top-headlines?q=" + search + "&category=" + category + "&apiKey=0425fffcba054e00a1e722ffb03f7ff5"
    val result = scala.io.Source.fromURL(url).mkString
    println(result)
  }
}