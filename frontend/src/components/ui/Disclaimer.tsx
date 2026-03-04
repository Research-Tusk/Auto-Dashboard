interface DisclaimerProps {
  className?: string;
  compact?: boolean;
}

export function Disclaimer({ className = '', compact = false }: DisclaimerProps) {
  if (compact) {
    return (
      <p className={`text-xs text-amber-700 ${className}`}>
        ⚠️ Demand proxy only — NOT accounting revenue.
      </p>
    );
  }

  return (
    <div className={`disclaimer-banner ${className}`}>
      <strong>⚠️ Important Disclaimer:</strong> All revenue figures are demand-based proxies
      calculated as <em>retail registrations × analyst ASP assumption</em>. This is{' '}
      <strong>NOT</strong> reported accounting revenue. Do not use for investment decisions
      without cross-referencing official OEM quarterly results.
    </div>
  );
}
