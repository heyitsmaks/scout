-- RLS governs rows, not table-wide operations. Browser roles need DML only.
REVOKE TRUNCATE, REFERENCES, TRIGGER ON TABLE
  public.profiles, public.saved_events, public.attended_events, public.feedback
  FROM PUBLIC, anon, authenticated;

-- Hosted projects may include this automatic RLS event trigger. It remains
-- usable by its owner; it does not need to be executable by browser roles.
DO $$
BEGIN
  IF to_regprocedure('public.rls_auto_enable()') IS NOT NULL THEN
    REVOKE EXECUTE ON FUNCTION public.rls_auto_enable() FROM PUBLIC, anon, authenticated;
  END IF;
  -- Some older hosted schemas use bigint IDs; fresh schemas use UUID IDs.
  IF to_regclass('public.saved_events_id_seq') IS NOT NULL THEN
    GRANT USAGE ON SEQUENCE public.saved_events_id_seq TO authenticated;
  END IF;
  IF to_regclass('public.attended_events_id_seq') IS NOT NULL THEN
    GRANT USAGE ON SEQUENCE public.attended_events_id_seq TO authenticated;
  END IF;
END
$$;
