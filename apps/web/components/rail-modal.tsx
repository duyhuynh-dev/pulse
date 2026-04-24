"use client";

import { useEffect, useId, useRef, useState, type ReactNode } from "react";
import { createPortal } from "react-dom";
import { X } from "lucide-react";

interface RailModalProps {
  open: boolean;
  title: string;
  onClose: () => void;
  children: ReactNode;
}

const OPEN_TRANSITION_MS = 220;
const CLOSE_TRANSITION_MS = 180;

export function RailModal({ open, title, onClose, children }: RailModalProps) {
  const [shouldRender, setShouldRender] = useState(open);
  const [isVisible, setIsVisible] = useState(false);
  const [hasMounted, setHasMounted] = useState(false);
  const closeButtonRef = useRef<HTMLButtonElement | null>(null);
  const dialogRef = useRef<HTMLDivElement | null>(null);
  const previouslyFocusedRef = useRef<HTMLElement | null>(null);
  const titleId = useId();

  useEffect(() => {
    setHasMounted(true);
  }, []);

  useEffect(() => {
    if (open) {
      previouslyFocusedRef.current = document.activeElement instanceof HTMLElement ? document.activeElement : null;
      setShouldRender(true);
      const frame = window.requestAnimationFrame(() => {
        setIsVisible(true);
      });
      return () => window.cancelAnimationFrame(frame);
    }

    if (!shouldRender) {
      return;
    }

    setIsVisible(false);
    const timeout = window.setTimeout(() => {
      setShouldRender(false);
      previouslyFocusedRef.current?.focus?.();
    }, CLOSE_TRANSITION_MS);

    return () => window.clearTimeout(timeout);
  }, [open, shouldRender]);

  useEffect(() => {
    if (!shouldRender) {
      return;
    }

    const originalOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";

    return () => {
      document.body.style.overflow = originalOverflow;
    };
  }, [shouldRender]);

  useEffect(() => {
    if (!shouldRender) {
      return;
    }

    const timeout = window.setTimeout(() => {
      closeButtonRef.current?.focus();
    }, 40);

    return () => window.clearTimeout(timeout);
  }, [shouldRender]);

  useEffect(() => {
    if (!shouldRender) {
      return;
    }

    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        event.preventDefault();
        onClose();
        return;
      }

      if (event.key !== "Tab" || !dialogRef.current) {
        return;
      }

      const focusable = Array.from(
        dialogRef.current.querySelectorAll<HTMLElement>(
          'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])'
        )
      ).filter((element) => !element.hasAttribute("disabled") && element.tabIndex !== -1);

      if (!focusable.length) {
        event.preventDefault();
        return;
      }

      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      const active = document.activeElement;

      if (event.shiftKey && active === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && active === last) {
        event.preventDefault();
        first.focus();
      }
    }

    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [onClose, shouldRender]);

  if (!hasMounted || !shouldRender) {
    return null;
  }

  return createPortal(
    <div
      className="fixed inset-0 z-[40] flex items-center justify-center p-4 sm:p-6"
    >
      <div
        aria-hidden="true"
        className="absolute inset-0 z-0 bg-black/30 transition-opacity duration-200"
        onClick={onClose}
        style={{
          opacity: isVisible ? 1 : 0,
          backdropFilter: isVisible ? "blur(12px)" : "blur(0px)",
          transition: `opacity 200ms ease, backdrop-filter 250ms ease`,
        }}
      />

      <div
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        className="relative z-[50] flex max-h-[calc(100vh-80px)] w-[min(640px,calc(100vw-32px))] flex-col overflow-hidden rounded-[2rem] border border-stroke/80 bg-card shadow-2xl"
        onClick={(event) => event.stopPropagation()}
        style={{
          opacity: isVisible ? 1 : 0,
          transform: isVisible ? "scale(1)" : "scale(0.96)",
          transition: `opacity ${open ? OPEN_TRANSITION_MS : CLOSE_TRANSITION_MS}ms ease-out, transform ${open ? OPEN_TRANSITION_MS : CLOSE_TRANSITION_MS}ms ease-out`,
        }}
      >
        <div className="sticky top-0 z-10 flex items-center justify-between gap-4 border-b border-stroke/70 bg-card/95 px-6 py-5 backdrop-blur">
          <h2 id={titleId} className="text-2xl font-semibold text-slate-900">
            {title}
          </h2>
          <button
            ref={closeButtonRef}
            type="button"
            aria-label="Close"
            onClick={onClose}
            className="inline-flex h-11 w-11 items-center justify-center rounded-full border border-stroke bg-white/80 text-slate-600 transition hover:bg-white hover:text-slate-900"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        <div className="min-h-0 overflow-y-auto px-6 py-5">{children}</div>
      </div>
    </div>,
    document.body
  );
}
