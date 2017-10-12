import org.junit.Test

/**
 * Created by owen on 17/10/9.
 */


class MySQLTest{

    @Test
    fun testInsert(){
        val stat = MySQLUtil.getStatement()
        val sql = "insert into actor(first_name,last_name) values('A','B')"
        MySQLUtil.executePlateSQL(stat, sql)
    }
}
