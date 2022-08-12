/* flatfile.nadac*/
TRUNCATE TABLE public.nadac;
INSERT INTO public.nadac
SELECT ndc || price_line::text AS ID,*
FROM flatfile.nadac;