package ucfs.claimant.consumer

import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.cache.annotation.EnableCaching
import org.springframework.retry.annotation.EnableRetry
import ucfs.claimant.consumer.orchestrate.Orchestrator
import javax.sql.DataSource
import kotlin.time.ExperimentalTime

@SpringBootApplication
@EnableCaching
@EnableRetry
class UcfsClaimantConsumer(private val orchestrator: Orchestrator, private val dataSource: DataSource) : CommandLineRunner {

    @ExperimentalTime
    override fun run(vararg args: String?) {
        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery("SELECT count(*) from claimant").use { results ->
                    if (results.next()) {
                        val count = results.getInt(1)
                        println("WOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO '$count'")
                    }
                }

            }
        }
//        orchestrator.orchestrate()
    }
}

fun main(args: Array<String>) {
    runApplication<UcfsClaimantConsumer>(*args)
}
