import scala.io.StdIn._
import helpers.apiConnect
import helpers.hiveConnect
//NOTES
// spark-submit --packages net.liftweb:lift-json_2.11:2.6 project-one_2.11-0.1.0-SNAPSHOT.jar

object Main {
  def main(args: Array[String]): Unit = {
    var loop = true
    println("##     ## ######## ##    ##  #######  ##     ## \n##     ## ##       ###   ## ##     ## ###   ### \n##     ## ##       ####  ## ##     ## #### #### \n##     ## ######   ## ## ## ##     ## ## ### ## \n ##   ##  ##       ##  #### ##     ## ##     ## \n  ## ##   ##       ##   ### ##     ## ##     ## \n   ###    ######## ##    ##  #######  ##     ## ")
    do{
      
      println("Please select an option")
      println("1. Log-in\n2. Quit Application")
      try {
      val option = readInt()
      println()
      option match{
        case 1 => {
          var loop2 = true
          val user = readLine("Username: ") // NOTE: Create username and password file IN HIVE and check and read from it to verify login.
          val passwrd = readLine("Password: ") // Make it so only ADMIN can login to import data.
          println()
          val hc = new hiveConnect(user,passwrd)
          hc.login()
          
        }
        case 2 => {
           loop = false
        }
      }
      }catch {
        case e: MatchError => println("Please pick a number between 1~8\n")
        case e: NumberFormatException => println("\nPlease enter a number\n") 
      }
      
    } while(loop) 
    println("Thank you")
  }
}