package ucfs.claimant.consumer.orchestrate.impl

import arrow.core.right
import com.google.gson.Gson
import com.google.gson.JsonObject
import com.nhaarman.mockitokotlin2.*
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import ucfs.claimant.consumer.domain.*
import ucfs.claimant.consumer.processor.CompoundProcessor
import ucfs.claimant.consumer.processor.PreProcessor
import ucfs.claimant.consumer.target.FailureTarget
import ucfs.claimant.consumer.target.SuccessTarget
import java.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.seconds
import kotlin.time.toJavaDuration

@ExperimentalTime
class OrchestratorFilterTest : StringSpec() {

    init {
        "Sends filtered updates to the success target" {
            val successTarget = mock<SuccessTarget>()
            val failureTarget = mock<FailureTarget>()
            with (orchestrator(consumerProvider(), mock(), compoundProcessor(), successTarget, failureTarget)) {
                shouldThrow<RuntimeException> { orchestrate() }
            }
            validateSuccesses(successTarget)
        }
    }

    private suspend fun validateSuccesses(successTarget: SuccessTarget) {
        verifyAdditionsAndModifications(successTarget)
        verifyDeletes(successTarget)
    }

    private suspend fun verifyAdditionsAndModifications(successTarget: SuccessTarget) {
        val topicCaptor = argumentCaptor<String>()
        argumentCaptor<List<TransformationProcessingResult>> {
            verify(successTarget, times(1)).upsert(topicCaptor.capture(), capture())
            topicCaptor.firstValue shouldBe TOPIC
            firstValue.size shouldBe 50 * 3 / 4 + 1
            firstValue.forEachIndexed { index, result ->
                with(result.first) {
                    String(key()).toInt() % 4 shouldNotBe 3
                    offset() % 4 shouldNotBe 3
                    topic() shouldBe TOPIC
                    partition() shouldBe 0
                }

                with(result.second.extract) {
                    id.toInt() % 4 shouldNotBe 3
                    action shouldBe if (index % 3 == 0) DatabaseAction.MONGO_INSERT else DatabaseAction.MONGO_UPDATE
                    timestampAndSource shouldBe Pair("2020-01-01", "_lastModifiedDateTime")
                }
            }
        }
    }

    private suspend fun verifyDeletes(successTarget: SuccessTarget) {
        val topicCaptor = argumentCaptor<String>()
        argumentCaptor<List<JsonProcessingResult>> {
            verify(successTarget, times(1)).delete(topicCaptor.capture(), capture())
            allValues.size shouldBe 1
            firstValue.size shouldBe 100 / 4
            firstValue.forEach { result ->
                with (result.first) {
                    String(key()).toInt() % 4 shouldBe 2
                }
                with (result.second) {
                    action shouldBe DatabaseAction.MONGO_DELETE
                }
            }
        }
    }


    private fun consumerProvider(): () -> KafkaConsumer<ByteArray, ByteArray> {
        val batch = consumerRecords(0, 99)
        val consumer = mock<KafkaConsumer<ByteArray, ByteArray>> {
            on { poll(any<Duration>()) } doReturn batch doThrow RuntimeException("End the loop")
            on { listTopics() } doReturn mapOf(TOPIC to listOf(mock()))
            on { subscription() } doReturn setOf(TOPIC)
        }
        return { consumer }
    }

    private fun compoundProcessor(): CompoundProcessor {
        val records = processingOutputs()
        return mock {
            on { process(any()) } doReturnConsecutively records
        }
    }

    private fun processingOutputs() =
        (0..99).map { recordNumber ->
                    Pair(consumerRecord(recordNumber), mongoInsert(recordNumber)).right()
        }

    private fun mongoInsert(recordNumber: Int) = FilterResult(transformationResult(recordNumber), true)

    private fun transformationResult(recordNumber: Int) =
        TransformationResult(JsonProcessingExtract(Gson().fromJson("""{ "body": "$recordNumber" }""", JsonObject::class.java), "$recordNumber",
            DatabaseAction.MONGO_INSERT, Pair("2020-01-01", "_lastModifiedDateTime")), "TRANSFORMED_DB_OBJECT")

    private fun orchestrator(provider: () -> KafkaConsumer<ByteArray, ByteArray>,
                            preProcessor: PreProcessor,
                            processor: CompoundProcessor,
                            successTarget: SuccessTarget,
                            failureTarget: FailureTarget): OrchestratorImpl {
        return OrchestratorImpl(provider, Regex(TOPIC), preProcessor, processor,
            10.seconds.toJavaDuration(), successTarget, failureTarget)
    }

    private fun consumerRecords(first: Int, last: Int): ConsumerRecords<ByteArray, ByteArray> =
            ConsumerRecords((first..last).map(::consumerRecord).groupBy {
                TopicPartition(it.topic(), it.partition())
            })

    private fun consumerRecord(recordNumber: Int): ConsumerRecord<ByteArray, ByteArray> =
            mock {
                on { key() } doReturn "$recordNumber".toByteArray()
                on { topic() } doReturn TOPIC
                on { partition() } doReturn 0
                on { offset() } doReturn recordNumber.toLong()
            }

    companion object {
        private const val TOPIC = "db.database.collection"
    }
}
