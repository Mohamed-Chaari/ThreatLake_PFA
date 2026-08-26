interface SearchInputProps {
  value: string;
  onChange: (value: string) => void;
  placeholder: string;
}

// The same small filter input, reused identically on the Alerts and
// Attackers tabs: client-side IP-substring filtering over whatever rows
// are already loaded in that tab - no API call, instant. Each tab keeps
// its own text state (they're filtering different row sets), this is
// just the shared UI/behavior.
export default function SearchInput({ value, onChange, placeholder }: SearchInputProps) {
  return (
    <div className="search-input-wrap">
      <input
        type="text"
        className="search-input"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
      />
      {value && (
        <button type="button" className="search-clear" onClick={() => onChange("")} aria-label="Clear search">
          ×
        </button>
      )}
    </div>
  );
}
