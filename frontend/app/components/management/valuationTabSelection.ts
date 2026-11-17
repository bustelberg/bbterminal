/** A drag may end with a click on a tab. Preserve its text selection instead of navigating.
 * Keyboard/assistive activation (detail = 0) must remain available while text is selected. */
export function selectingTabText(event: { currentTarget: HTMLElement; detail: number }): boolean {
  if (event.detail === 0) return false;
  const selection = event.currentTarget.ownerDocument.getSelection();
  if (!selection || selection.isCollapsed || !selection.toString()) return false;
  for (let index = 0; index < selection.rangeCount; index++) {
    if (selection.getRangeAt(index).intersectsNode(event.currentTarget)) return true;
  }
  return false;
}
