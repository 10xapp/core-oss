export type ProjectPriorityValue = 0 | 1 | 2 | 3 | 4;

export interface ProjectPriorityOption {
  value: ProjectPriorityValue;
  label: string;
  shortLabel: string;
  color: string;
  bg: string;
  activeBg: string;
}

export const PROJECT_PRIORITY_OPTIONS = [
  {
    value: 0,
    label: 'No priority',
    shortLabel: 'None',
    color: 'text-gray-400',
    bg: 'bg-gray-50',
    activeBg: 'bg-gray-100 ring-1 ring-gray-200',
  },
  {
    value: 1,
    label: 'Urgent',
    shortLabel: 'P1',
    color: 'text-rose-500',
    bg: 'bg-rose-50',
    activeBg: 'bg-rose-100 ring-1 ring-rose-200',
  },
  {
    value: 2,
    label: 'High',
    shortLabel: 'P2',
    color: 'text-orange-500',
    bg: 'bg-orange-50',
    activeBg: 'bg-orange-100 ring-1 ring-orange-200',
  },
  {
    value: 3,
    label: 'Medium',
    shortLabel: 'P3',
    color: 'text-amber-500',
    bg: 'bg-amber-50',
    activeBg: 'bg-amber-100 ring-1 ring-amber-200',
  },
  {
    value: 4,
    label: 'Low',
    shortLabel: 'P4',
    color: 'text-slate-500',
    bg: 'bg-slate-100',
    activeBg: 'bg-slate-100 ring-1 ring-slate-200',
  },
] as const satisfies readonly ProjectPriorityOption[];
