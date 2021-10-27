import scala.io.StdIn._
import helpers.apiConnect
import helpers.hiveConnect

object Main {
  def main(args: Array[String]): Unit = {
    var loop = true
   
    do{
      
      println("Please select an option")
      println("1. Log-in\n2. Quit Application")
      try {
      val option = readInt()
      println()
      option match{
        case 1 => {
          val user = readLine("Username: ")
          val passwrd = readLine("Password: ")
        }
        case 3 => {
          val api = new apiConnect()
          api.searchEverything("game", "","" ,"","","", "1")
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