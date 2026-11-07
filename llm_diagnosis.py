
import os
import json
import re

from dotenv import load_dotenv
from groq import Groq


class LLMDiagnosis:
    """
    Generates AI-based log diagnoses using GroqCloud.
    """

    def __init__(self):

        load_dotenv()

        api_key = os.getenv("GROQ_API_KEY")

        self.model = os.getenv(
            "GROQ_MODEL",
            "openai/gpt-oss-120b"
        )

        if not api_key:
            raise ValueError(
                "GROQ_API_KEY not found in .env file."
            )

        self.client = Groq(api_key=api_key)

        print("[LLM] Groq client initialized successfully.")

    def build_system_prompt(self):

        return """
You are an experienced Site Reliability Engineer
and log analysis assistant.

Analyze application failures using the current log
and retrieved historical log evidence.

Rules:
1. Analyze the current log carefully.
2. Use historical logs only as supporting examples.
3. Historical logs are NOT verified root causes.
4. Never invent technical details.
5. Distinguish possible causes from confirmed causes.
6. If evidence is insufficient, explicitly say so.
7. Recommend practical troubleshooting steps.
8. Never expose passwords, tokens, or other secrets.
9. Return a valid JSON object only.
10. Do not use Markdown formatting around JSON keys.

Use exactly this JSON structure:

{
    "problem_summary": "Short explanation",
    "severity": "CRITICAL/HIGH/MEDIUM/LOW/UNKNOWN",
    "possible_causes": [
        {
            "cause": "Possible cause",
            "supporting_evidence": "Available evidence",
            "verification_steps": "How to verify"
        }
    ],
    "confirmed_causes": [],
    "recommended_actions": [
        "Action 1",
        "Action 2"
    ],
    "confidence": "LOW/MEDIUM/HIGH",
    "limitations": "What cannot be determined from the evidence"
}

Do not mark a possible cause as confirmed
without direct supporting evidence.
"""

    def parse_response(self, content):
        """
        Parse and normalize the JSON returned by the LLM.
        """

        # Remove possible Markdown code fences.
        content = re.sub(
            r"```(?:json)?",
            "",
            content,
            flags=re.IGNORECASE
        ).replace("```", "").strip()

        # First attempt: parse the complete response.
        try:
            return json.loads(content)

        except json.JSONDecodeError:
            pass

        # Extract JSON if extra text surrounds it.
        start = content.find("{")
        end = content.rfind("}")

        if start == -1 or end <= start:
            return {
                "raw_response": content,
                "parse_error": "No JSON object found."
            }

        json_content = content[start:end + 1]

        # Remove accidental Markdown emphasis before JSON keys.
        json_content = re.sub(
            r'\*+(?=")',
            '',
            json_content
        )

        try:
            result = json.loads(json_content)

            # Normalize a common field-name typo.
            for cause in result.get("possible_causes", []):

                if not isinstance(cause, dict):
                    continue

                if (
                    "supportual_evidence" in cause
                    and "supporting_evidence" not in cause
                ):
                    cause["supporting_evidence"] = cause.pop(
                        "supportual_evidence"
                    )

            return result

        except (json.JSONDecodeError, AttributeError, TypeError):

            return {
                "raw_response": content,
                "parse_error": "The model did not return valid JSON."
            }

    def diagnose(self, rag_context):
        """
        Send RAG context to GroqCloud and generate a diagnosis.
        """

        if (
            not isinstance(rag_context, str)
            or not rag_context.strip()
        ):
            raise ValueError("RAG context cannot be empty.")

        print("\n[LLM] Sending RAG context to GroqCloud...")

        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {
                    "role": "system",
                    "content": self.build_system_prompt()
                },
                {
                    "role": "user",
                    "content": (
                        "Analyze the following log information:\n\n"
                        + rag_context
                    )
                }
            ],
            max_completion_tokens=1500
        )

        content = response.choices[0].message.content

        if not content:
            raise RuntimeError(
                "The LLM returned an empty response."
            )

        print("[LLM] Diagnosis generated successfully.")

        return self.parse_response(content)

    def close(self):
        """Close the Groq client."""

        self.client.close()


if __name__ == "__main__":

    diagnosis_engine = LLMDiagnosis()

    sample_context = """
CURRENT LOG UNDER ANALYSIS
---------------------------
Service: payment-service
Level: ERROR
Classification: APPLICATION_ERROR
Message: Payment transaction could not be completed

RELEVANT HISTORICAL LOGS
------------------------
Historical Log 1:
Service: payment-service
Level: ERROR
Classification: APPLICATION_ERROR
Message: Payment processing failed
Relevance score: 0.93

ANALYSIS INSTRUCTIONS
---------------------
Historical logs are examples, not verified root causes.
Distinguish possible causes from confirmed causes.
Do not invent missing technical details.
"""

    try:

        result = diagnosis_engine.diagnose(sample_context)

        print("\n========== AI LOG DIAGNOSIS ==========\n")

        print(
            json.dumps(
                result,
                indent=4,
                ensure_ascii=False
            )
        )

    finally:
        diagnosis_engine.close()
