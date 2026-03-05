interface DisclaimerProps {
  variant?: 'inline' | 'banner';
}

export function Disclaimer({ variant = 'inline' }: DisclaimerProps) {
  const text =
    'Revenue figures are demand-side estimates (registrations × ASP proxy) and do NOT represent reported financials. For investment decisions, refer to official company filings.';

  if (variant === 'banner') {
    return (
      <div className="rounded-lg bg-amber-50 border border-amber-200 px-4 py-3 text-xs text-amber-800 leading-relaxed">
        <span className="font-semibold">Disclaimer: </span>{text}
      </div>
    );
  }

  return (
    <p className="text-xs text-slate-400 italic leading-relaxed">
      <span className="font-medium not-italic">Note: </span>{text}
    </p>
  );
}
