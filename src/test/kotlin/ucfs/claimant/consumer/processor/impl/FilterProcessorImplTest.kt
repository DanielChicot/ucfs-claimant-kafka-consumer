package ucfs.claimant.consumer.processor.impl

import com.google.gson.JsonObject
import com.nhaarman.mockitokotlin2.mock
import io.kotest.assertions.arrow.either.shouldBeRight
import io.kotest.core.spec.style.StringSpec
import io.kotest.data.forAll
import io.kotest.data.row
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.property.checkAll
import ucfs.claimant.consumer.domain.DatabaseAction
import ucfs.claimant.consumer.domain.JsonProcessingExtract
import ucfs.claimant.consumer.domain.TransformationProcessingResult
import ucfs.claimant.consumer.domain.TransformationResult

class FilterProcessorImplTest: StringSpec() {

    init {
        "Return true if not claimant" {
            checkAll<String> { whatever ->
                forAll(*nonClaimant.map(::row).toTypedArray()) { topic ->
                    allowedThrough(whatever).shouldBeRight {
                        it.second.passThrough.shouldBeTrue()
                    }
                }
            }
        }

        "Return true if claimant and nino not blank" {
            allowedThrough("""{ "nino": "123" }""").shouldBeRight {
                it.second.passThrough.shouldBeFalse()
            }
        }

        "Return false if claimant and nino empty" {
            allowedThrough("""{ "nino": "" }""").shouldBeRight {
                it.second.passThrough.shouldBeFalse()
            }
        }

        "Return false if claimant and malformed" {
            allowedThrough("""{ "nino": }""").shouldBeRight {
                it.second.passThrough.shouldBeFalse()
            }
        }

        "Return false if claimant and nino blank" {
            allowedThrough("""{ "nino": "   " }""").shouldBeRight {
                it.second.passThrough.shouldBeFalse()
            }
        }

        "Return false if claimant and nino absent" {
            allowedThrough("{}").shouldBeRight {
                it.second.passThrough.shouldBeFalse()
            }
        }
    }

    companion object {
        private fun allowedThrough(json: String) =
            FilterProcessorImpl(claimantTopic).process(processingResult(json))

        private fun processingResult(json: String): TransformationProcessingResult =
            TransformationProcessingResult(mock(), transformationResult(json))

        private fun transformationResult(json: String): TransformationResult = TransformationResult(extract, json)
        private val extract = JsonProcessingExtract(JsonObject(), "", DatabaseAction.MONGO_UPDATE, Pair("", ""))
        private const val claimantTopic = "db.core.claimant"
        private const val contractTopic = "db.core.contract"
        private const val statementTopic = "db.core.statement"
        private val nonClaimant = listOf(contractTopic, statementTopic)
    }
}
