/** Empty state for charts refreshed together from the modal header. */
export default function MissingFundamentals({ message }: { message: string }) {
  return (
    <div className="py-16 flex flex-col items-center gap-3 text-center px-4">
      <p className="text-[12px] text-fg-faint">{message}</p>
    </div>
  );
}
