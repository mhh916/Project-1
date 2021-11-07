package helpers

class Cipher(var password: String) {
    
    val shift = 2

    
    def getEncryptedPassword(): String = {
        encrypt(shift,password)
    }

    def getDecryptedPassword(): String = {
        decrypt(shift, password)
    }
    // Shifts the ASCII values of the characters  by the shftAmt val
    def encrypt(shftAmt: Int, pass: String): String = {
        val charArray = pass.toCharArray()
        var ret = ""
        for (c <- charArray) {
            if(c.toInt == 32){
                ret += " "
            }else{
                var temp = (c.toInt + shftAmt)
				if (temp < 33) {
					temp = 127 - (33 - temp)
				}
                if (temp > 126) {
                    temp = 32 + (temp - 126)
				}
				ret += temp.toChar
            }
        }
        ret
    }
    // Passes the shftAmt value to encrypt but as a negative value.
    def decrypt(shftAmt: Int, pass: String): String = {
        return encrypt((shift * - 1), pass)
    }
}