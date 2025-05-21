CREATE SCHEMA trab_email;
SET search_path TO trab_email;

create table tb_palavra (
	ds_palavra varchar(30)
);

alter table tb_palavra
add constraint pk_palavra primary key (ds_palavra);

create table tb_usuario (
	id_usuario integer,
	nm_usuario varchar (60) not null
);

alter table tb_usuario
add constraint pk_usuario primary key (id_usuario);

create table tb_email (
	id_email integer,
	ds_texto varchar not null,
	dh_email timestamp not null,
	id_usuario integer not null
);

alter table tb_email
add constraint pk_email primary key (id_email);

alter table tb_email
add constraint fk_email_usuario foreign key (id_usuario) references tb_usuario;

create table tb_quarentena (
	id_email integer,
	ds_palavra varchar(30) not null
);

alter table tb_quarentena 
add constraint pk_quarentena primary key (id_email);

alter table tb_quarentena
add constraint fk_quarentena_email foreign key (id_email) references tb_quarentena;

alter table tb_quarentena
add constraint fk_quarentena_palavra foreign key (ds_palavra) references tb_palavra;


create or replace function fn_normaliza(p_texto varchar)
returns varchar
language plpgsql
as

$$
declare
	txt varchar;
	
begin
	txt := trim(p_texto);
	txt := translate(txt, 'ÁÀÂÃÉÈÊÍÌÎÓÒÔÕÚÙÛÇáàâãéèêíìîóòôõúùûç','AAAAEEEIIIOOOOUUUCaaaaeeeiiioooouuuc'); 
	txt := regexp_replace(txt, '\s+', ' ', 'g');
	txt := upper(txt);
	
	raise notice '%', txt;
	return txt;
end;
$$

create or replace function fn_localiza(p_texto varchar, p_chave varchar)
returns boolean
language plpgsql
as

$$
declare
	txt_normalizado 	varchar;
	busca_normalizado 	varchar;
	elementos 			text[];
	i 					integer;
begin
	txt_normalizado := fn_normaliza(p_texto);
	busca_normalizado := fn_normaliza(p_chave);
	
	elementos := string_to_array(txt_normalizado, ' ');
	
	for i in 1..array_length(elementos, 1) loop
			
		raise notice '%', elementos[i];
		
		if elementos[i] = busca_normalizado then
			return true;
		end if;
	end loop;
	
	return false;
end;
$$

create or replace function fn_normaliza_palavra()
returns trigger
language plpgsql

as 
$$
begin
	NEW.ds_palavra := fn_normaliza(NEW.ds_palavra);
	return NEW;
end;
$$

create trigger tg_bins_palavra
before insert on tb_palavra
for each row
execute function fn_normaliza_palavra();

create or replace function fn_seguranca_palavra()
returns trigger
language plpgsql

as
$$
declare 
	verifica integer;
begin
	select count(*) into verifica from tb_quarentena where ds_palavra = OLD.ds_palavra;
	
	if verifica != 0 then
		raise exception 'Existem e-mails em quarentena com essa palavra';
	end if;
	
	return OLD;
end;
$$

create trigger tg_up_del_palavra
before update or delete on tb_palavra
for each row
execute function fn_seguranca_palavra();

create or replace function fn_normaliza_usuario()
returns trigger
language plpgsql

as
$$
begin
	NEW.nm_usuario := fn_normaliza(NEW.nm_usuario);
	return NEW;
end;
$$

create trigger tg_bins_usuario
before insert on tb_usuario
for each row
execute function fn_normaliza_usuario();


create or replace function fn_seguranca_usuario_up()
returns trigger
language plpgsql

as
$$
declare 
	verifica_user integer;
begin
	select count(*) into verifica_user from tb_email where OLD.id_usuario = id_usuario;
	
	if verifica_user != 0 then
		raise exception 'Esse usuário possui e-mails, não podendo ser alterado';
	else
		if NEW.id_usuario != OLD.id_usuario then
			raise exception 'O Id do usuário não pode ser alterado';
		else
			NEW.nm_usuario := fn_normaliza(NEW.nm_usuario);
			raise notice '%', NEW.nm_usuario;
			return NEW;
		end if;
	end if;
end;
$$

create trigger tg_bup_usuario
before update on tb_usuario
for each row
execute function fn_seguranca_usuario_up();

create or replace function fn_seguranca_usuario_del()
returns trigger
language plpgsql

as
$$
declare 
	verifica_user integer;
begin
	select count(*) into verifica_user from tb_email where OLD.id_usuario = id_usuario;
	
	if verifica_user != 0 then
		raise exception 'Esse usuário possui e-mails, não podendo ser deletado';
	else 
		return OLD;
	end if;
end;
$$

create trigger tg_bdel_usuario
before delete on tb_usuario
for each row
execute function fn_seguranca_usuario_del();

create or replace function fn_seguranca_email()
returns trigger	
language plpgsql
as

$$
begin
	raise exception 'Esse comando não é permitido nessa tabela.';
end;
$$

create trigger tg_bdelup_email
before delete or update on tb_email
for each row
execute function fn_seguranca_email();

create sequence id_email;

create or replace function fn_normaliza_email()
returns trigger
language plpgsql
as

$$
begin
	NEW.dh_email := current_timestamp;
	NEW.ds_texto := fn_normaliza(NEW.ds_texto);
	NEW.id_email := nextval('id_email');
	
	return NEW;
end;
$$

create trigger tg_bins_email
before insert on tb_email
for each row
execute function fn_normaliza_email();

create or replace function fn_quarentena()
returns trigger
language plpgsql
as

$$
declare 
	palavras 	   RECORD;
	existe_palavra integer;
begin
	for palavras in select ds_palavra from tb_palavra loop
		if fn_localiza(NEW.ds_texto, palavras.ds_palavra) then
		
			select count(*) into existe_palavra from tb_quarentena where id_email = NEW.id_email and ds_palavra = palavras.ds_palavra;
			
			if existe_palavra != 0 then
				continue;
			else
				insert into tb_quarentena values(NEW.id_email, palavras.ds_palavra);
			end if;
		end if;
	end loop;
	
	return NEW;
end;
$$

create trigger tg_afins_email
after insert on tb_email
for each row
execute function fn_quarentena();
