import partner.JobRequest
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement

/**
 * Created by owen on 17/10/9.
 */
class MySQLUtil{


    companion object{

        init {
            DriverManager.registerDriver(com.mysql.jdbc.Driver())
        }
        var conn: Connection? = null
        const val URL = "jdbc:mysql://localhost:3306/sakila"
        const val USER = "root"
        const val PASSWORD = "123456"


        fun getStatement(): Statement{
            if (conn == null){
                conn = DriverManager.getConnection(URL, USER, PASSWORD)
            }
            return conn!!.createStatement()
        }

        fun fakeExecutorSQL(task: JobRequest){
            println(task.sql+"---->")
        }

        fun executeSQL(task: JobRequest): Boolean{
            val stat = getStatement()
            return executePlateSQL(stat,task.sql)
        }

        fun executePlateSQL(stat: Statement,sql: String): Boolean{
            return stat.execute(sql)
        }


    }

}