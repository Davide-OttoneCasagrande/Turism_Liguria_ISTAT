ALTER TABLE public."Capacity_establishments"
ADD CONSTRAINT capacity_of_establishments_dim_cl_freq_fk FOREIGN KEY ("FREQ") REFERENCES public.dim_cl_freq(id),
ADD CONSTRAINT capacity_of_establishments_dim_cl_itter107_fk FOREIGN KEY ("REF_AREA") REFERENCES public.dim_cl_itter107(id),
ADD CONSTRAINT capacity_of_establishments_dim_cl_correz_fk FOREIGN KEY ("ADJUSTMENT") REFERENCES public.dim_cl_correz(id),
ADD CONSTRAINT capacity_of_establishments_dim_cl_tipo_alloggio2_fk FOREIGN KEY ("TYPE_ACCOMMODATION") REFERENCES public.dim_cl_tipo_alloggio2(id),
ADD CONSTRAINT capacity_of_establishments_dim_cl_ateco_2007_fk FOREIGN KEY ("ECON_ACTIVITY_NACE_2007") REFERENCES public.dim_cl_ateco_2007(id),
ADD CONSTRAINT capacity_of_establishments_dim_cl_iso_fk FOREIGN KEY ("COUNTRY_RES_GUESTS") REFERENCES public.dim_cl_iso(id),
ADD CONSTRAINT capacity_of_establishments_dim_cl_tipoitter1_fk FOREIGN KEY ("LOCALITY_TYPE") REFERENCES public.dim_cl_tipoitter1(id),
ADD CONSTRAINT capacity_of_establishments_dim_cl_tipoitter1_fk_1 FOREIGN KEY ("URBANIZ_DEGREE") REFERENCES public.dim_cl_tipoitter1(id),
ADD CONSTRAINT capacity_of_establishments_dim_cl_tipoitter1_fk_2 FOREIGN KEY ("COASTAL_AREA") REFERENCES public.dim_cl_tipoitter1(id),
ADD CONSTRAINT capacity_of_establishments_dim_cl_numerosita_fk FOREIGN KEY ("SIZE_BY_NUMBER_ROOMS") REFERENCES public.dim_cl_numerosita(id);