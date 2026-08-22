import { useState } from "react";
import { askCopilot, type CopilotQueryResponse } from "./api";

interface ChatTurn {
  question: string;
  response: CopilotQueryResponse;
}

// A chat-shaped front end over POST /copilot/query. Every question's
// response is shown in full - the generated SQL and either the returned
// rows or the rejection reason - so a viewer can see exactly what
// threatlake.copilot.guardrails did with each question, not just a
// final answer.
export default function CopilotView() {
  const [question, setQuestion] = useState("");
  const [history, setHistory] = useState<ChatTurn[]>([]);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function handleSubmit(event: React.FormEvent) {
    event.preventDefault();
    if (!question.trim() || pending) {
      return;
    }
    setPending(true);
    setError(null);
    try {
      const response = await askCopilot(question);
      setHistory((prev) => [...prev, { question, response }]);
      setQuestion("");
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setPending(false);
    }
  }

  return (
    <div className="copilot-view">
      <div className="copilot-history">
        {history.length === 0 && (
          <p className="hint">
            Ask a question about attacker_profiles or attack_timeline, e.g. "which
            5 IPs have the most events?"
          </p>
        )}
        {history.map((turn, i) => (
          <div key={i} className="copilot-turn">
            <p className="copilot-question">{turn.question}</p>
            {turn.response.rejected ? (
              <p className="error">Rejected: {turn.response.reason}</p>
            ) : (
              <>
                <pre className="copilot-sql">{turn.response.sql}</pre>
                <p>{turn.response.row_count} row(s) returned.</p>
                <pre className="copilot-rows">
                  {JSON.stringify(turn.response.rows, null, 2)}
                </pre>
              </>
            )}
          </div>
        ))}
      </div>

      <form onSubmit={handleSubmit} className="copilot-form">
        <input
          type="text"
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          placeholder="Ask the copilot a question..."
          disabled={pending}
        />
        <button type="submit" disabled={pending}>
          {pending ? "Asking..." : "Ask"}
        </button>
      </form>
      {error && <p className="error">{error}</p>}
    </div>
  );
}
