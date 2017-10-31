package partner

/**
 * Created by owen on 17/10/9.
 */

enum class JobType{
    UPDATE,INSERT,SELECT,DELETE
}

class JobRequest(val type: JobType, val sql: String){
    companion object {
        fun toJobRequest(sql: String): JobRequest{
            val request =
            when {
                sql.startsWith("insert", true)
                        -> JobRequest(JobType.INSERT, sql)
                sql.startsWith("update", true)
                        -> JobRequest(JobType.UPDATE, sql)
                sql.startsWith("delete", true)
                        -> JobRequest(JobType.DELETE, sql)
                sql.startsWith("select", true)
                        -> JobRequest(JobType.SELECT, sql)
                else -> throw IllegalArgumentException("invalid sql")
            }

            return request
        }
    }
}

