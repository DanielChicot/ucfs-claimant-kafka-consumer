package ucfs.claimant.consumer.configuration

import arrow.core.Either
import arrow.core.extensions.either.applicative.applicative
import arrow.core.fix
import arrow.core.flatMap
import arrow.core.identity
import org.apache.commons.dbcp2.BasicDataSource
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ucfs.claimant.consumer.repository.SecretRepository
import ucfs.claimant.consumer.utility.GsonExtensions.integer
import ucfs.claimant.consumer.utility.GsonExtensions.jsonObject
import ucfs.claimant.consumer.utility.GsonExtensions.string
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.io.File
import javax.sql.DataSource
import kotlin.time.ExperimentalTime

@Configuration
class RdsConfiguration(private val databaseCaCertPath: String,
                       private val claimantTable: String,
                       private val contractTable: String,
                       private val statementTable: String,
                       private val claimantNaturalIdField: String,
                       private val contractNaturalIdField: String,
                       private val statementNaturalIdField: String) {

    @ExperimentalTime
    @Bean
    fun dataSource(secretRepository: SecretRepository, rdsSecretName: String): DataSource =
        secretRepository.secret(rdsSecretName).jsonObject().flatMap {
            Either.applicative<Any>().tupledN(it.string("dbInstanceIdentifier"),
                                              it.string("host"),
                                              it.integer("port"),
                                              it.string("username"),
                                              it.string("password")).fix()
        }.map { (instance, host, port, username, password) ->
            BasicDataSource().apply {
                driverClassName = "com.mysql.cj.jdbc.Driver"
                addConnectionProperty("user", username)
                addConnectionProperty("password", password)
                log.info("CA Certificate path", "path" to databaseCaCertPath, "exists" to "${File(databaseCaCertPath).isFile}")
//                if (databaseCaCertPath.isNotBlank()) {
                    url = "jdbc:mysql://$host:$port/$instance?enabledTLSProtocols=TLSv1.2"
                    log.info("Database url", "url" to url)
                    addConnectionProperty("enabledTLSProtocols", "TLSv1.2")
                    addConnectionProperty("sslMode", "REQUIRED")
//                    addConnectionProperty("clientCertificateKeyStoreUrl", "file:./$databaseCaCertPath")
//                    addConnectionProperty("clientCertificateKeyStorePassword", "password")
                    addConnectionProperty("trustCertificateKeyStoreUrl", "file:./$databaseCaCertPath")
                    addConnectionProperty("trustCertificateKeyStorePassword", "password")
//                }
//                else {
//                    url = "jdbc:mysql://$host:$port/$instance"
//                    addConnectionProperty("useSSL", "false")
//                }
                println("BasicDataSource: $this")
            }

        }.fold(
            ifRight = ::identity,
            ifLeft = {
                throw RuntimeException("Failed to parse required connection parameters from secret '$rdsSecretName' value")
            })

    @Bean
    @Qualifier("targetTables")
    fun targetTables(claimantTopic: String, contractTopic: String, statementTopic: String): Map<String, String> =
            mapOf(claimantTopic to claimantTable, contractTopic to contractTable, statementTopic to statementTable)


    @Bean
    @Qualifier("naturalIdFields")
    fun naturalIdFields(claimantTopic: String, contractTopic: String, statementTopic: String) =
            mapOf(claimantTopic to claimantNaturalIdField, contractTopic to contractNaturalIdField, statementTopic to statementNaturalIdField)

    companion object {
        private val log = DataworksLogger.getLogger(RdsConfiguration::class)
    }
}
