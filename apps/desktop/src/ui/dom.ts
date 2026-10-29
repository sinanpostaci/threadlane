export type ChildNodeSpec = Node | string | number | null | undefined | false;
export type ElementAttrs = Record<string, string | number | boolean | null | undefined>;

export function h<K extends keyof HTMLElementTagNameMap>(
  tag: K,
  attrs?: ElementAttrs,
  ...children: ChildNodeSpec[]
): HTMLElementTagNameMap[K] {
  const element = document.createElement(tag);
  if (attrs) {
    for (const [key, value] of Object.entries(attrs)) {
      if (value === undefined || value === null || value === false) {
        continue;
      }
      if (key === 'className' || key === 'class') {
        element.className = String(value);
      } else if (
        key === 'value' &&
        (element instanceof HTMLInputElement ||
          element instanceof HTMLSelectElement ||
          element instanceof HTMLTextAreaElement)
      ) {
        element.value = String(value);
        element.setAttribute(key, String(value));
      } else if (key === 'checked' && element instanceof HTMLInputElement) {
        element.checked = Boolean(value);
        if (value === true) {
          element.setAttribute(key, '');
        }
      } else if (key === 'selected' && element instanceof HTMLOptionElement) {
        element.selected = Boolean(value);
        if (value === true) {
          element.setAttribute(key, '');
        }
      } else if (
        key === 'disabled' &&
        (element instanceof HTMLButtonElement ||
          element instanceof HTMLInputElement ||
          element instanceof HTMLSelectElement ||
          element instanceof HTMLTextAreaElement ||
          element instanceof HTMLOptionElement ||
          element instanceof HTMLFieldSetElement)
      ) {
        element.disabled = Boolean(value);
        if (value === true) {
          element.setAttribute(key, '');
        }
      } else if (value === true) {
        element.setAttribute(key, '');
      } else {
        element.setAttribute(key, String(value));
      }
    }
  }
  for (const child of children) {
    if (child === null || child === undefined || child === false) {
      continue;
    }
    if (child instanceof Node) {
      element.appendChild(child);
    } else {
      element.appendChild(document.createTextNode(String(child)));
    }
  }
  return element;
}

export function createButton(
  text: string,
  onClick: () => void,
  disabled: boolean = false
): HTMLButtonElement {
  const button = h(
    'button',
    {
      type: 'button',
      className: 'aq-btn',
      disabled: Boolean(disabled),
    },
    text
  );
  button.addEventListener('click', () => {
    if (!button.disabled) {
      onClick();
    }
  });
  return button;
}
