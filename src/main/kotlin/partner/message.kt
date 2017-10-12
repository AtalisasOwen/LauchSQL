package partner

/**
 * Created by owen on 17/10/9.
 */

enum class JobType{
    UPDATE,INSERT,SELECT,DELETE
}

class JobRequest(val type: JobType, val sql: String)

