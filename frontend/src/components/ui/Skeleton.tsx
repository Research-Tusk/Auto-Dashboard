interface SkeletonProps {
  className?: string;
  lines?: number;        // for text skeleton
  type?: 'text' | 'card' | 'chart' | 'table';
}

export function Skeleton({ className = '', type = 'text', lines = 3 }: SkeletonProps) {
  if (type === 'chart') {
    return <div className={`skeleton h-64 w-full rounded-xl ${className}`} />;
  }

  if (type === 'card') {
    return (
      <div className={`kpi-card ${className}`}>
        <div className="skeleton h-8 w-24 mb-2" />
        <div className="skeleton h-4 w-16" />
      </div>
    );
  }

  if (type === 'table') {
    return (
      <div className={`space-y-2 ${className}`}>
        <div className="skeleton h-10 w-full rounded" />
        {[...Array(lines)].map((_, i) => (
          <div key={i} className="skeleton h-8 w-full rounded" />
        ))}
      </div>
    );
  }

  // Default: text skeleton
  return (
    <div className={`space-y-2 ${className}`}>
      {[...Array(lines)].map((_, i) => (
        <div
          key={i}
          className={`skeleton h-4 rounded ${i === lines - 1 ? 'w-3/4' : 'w-full'}`}
        />
      ))}
    </div>
  );
}
