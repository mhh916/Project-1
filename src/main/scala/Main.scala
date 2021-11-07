import scala.io.StdIn._
import helpers.apiConnect
import helpers.hiveConnect
object Main {
  def main(args: Array[String]): Unit = {
    var loop = true
    // Venom ASCII Logo
    println("##     ## ######## ##    ##  #######  ##     ## \n##     ## ##       ###   ## ##     ## ###   ### \n##     ## ##       ####  ## ##     ## #### #### \n##     ## ######   ## ## ## ##     ## ## ### ## \n ##   ##  ##       ##  #### ##     ## ##     ## \n  ## ##   ##       ##   ### ##     ## ##     ## \n   ###    ######## ##    ##  #######  ##     ## ")
    // Main for loop for login and quiting application
    do{
      
      println("Please select an option")
      println("1. Log-in\n2. Quit Application")
      try {
      val option = readInt()
      println()
      option match{
        case 1 => {
          var loop2 = true
          val user = scala.io.StdIn.readLine("Username: ")
          val passwrd = scala.io.StdIn.readLine("Password: ")
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