import { type LucideIcon, type LucideProps } from "lucide-react";

type IconProps = LucideProps & {
  icon: LucideIcon;
  active?: boolean;
};

export function Icon({ icon: IconComponent, strokeWidth = 1.6, active, ...props }: IconProps) {
  return (
    <IconComponent
      strokeWidth={active ? 1.8 : strokeWidth}
      {...props}
    />
  );
}

export type { LucideIcon };
