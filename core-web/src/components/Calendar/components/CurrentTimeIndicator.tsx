import { useEffect, useState } from 'react';

interface CurrentTimeIndicatorProps {
  hourHeight?: number;
  timeColumnWidth?: number;
}

export default function CurrentTimeIndicator({
  hourHeight = 60,
  timeColumnWidth = 53
}: CurrentTimeIndicatorProps) {
  const [currentTime, setCurrentTime] = useState(new Date());

  useEffect(() => {
    const timer = setInterval(() => {
      setCurrentTime(new Date());
    }, 60000); // Update every minute

    return () => clearInterval(timer);
  }, []);

  const hours = currentTime.getHours();
  const minutes = currentTime.getMinutes();
  const topPosition = (hours * 60 + minutes) * (hourHeight / 60);

  const circleSize = 8;
  // The visible grid line (where hour borders start) is at timeColumnWidth
  // To center the circle on it: left edge of circle = gridLine - radius
  const gridLine = timeColumnWidth;

  return (
    <div
      className="absolute pointer-events-none z-[110]"
      style={{
        top: topPosition,
        left: 0,
        right: 0
      }}
    >
      {/* Orange circle - centered on the grid line */}
      <div
        className="absolute bg-orange-500 rounded-full"
        style={{
          width: circleSize,
          height: circleSize,
          top: -circleSize / 2,
          left: gridLine - circleSize
        }}
      />
      {/* Orange line - from circle to right edge */}
      <div
        className="absolute bg-orange-500"
        style={{
          height: 2,
          left: gridLine,
          right: -16,
          top: -1
        }}
      />
    </div>
  );
}
