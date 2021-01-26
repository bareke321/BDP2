#!/bin/bash

# argument wejœciowy 0 - nazwa skryptu
# argument wejœciowy 1 - link do url
# argument wejœciowy 2 - has³o do zipa
# argument wejœciowy 3 - liczba kolumn w pobranym pliku
# argument wejœciowy 4- wartoœæ odciêcia dla kolumny OrderQuantity
# argument wejœciowy 5 - sciezka do pliku InternetSales_old.txt 
# argument wejœciowy 6- indeks
# argument wejœciowy 7- login do bazy danych
# argument wejœciowy 8- has³o zakodowane w base64
# argument wejœciowy 9 - adres hosta
# argument wejœciowy 10 - adres email



mkdir PROCESSED
TMSTP=$(date '+%Y%m%d')

log_flnm="$0_$TMSTP.log"
touch PROCESSED/$log_flnm

# wget do pobrania pliku -q ciche

wget -q "$1" 


Date_Stamp=$(date '+%Y%m%d%H%M%S')
echo "$Date_Stamp - Download - Succes" >>PROCESSED/"$log_flnm"


# wypakowanie zipa -q ciche, P zip z has³em


flnm=$(basename "$1")
password=$"$2" 
unzip -qP "$password" $flnm   

Date_Stamp=$(date '+%Y%m%d%H%M%S')
echo "$Date_Stamp - Extract - Succes" >>PROCESSED/"$log_flnm"


# Usuwanie pustych lini, wc -l liczenie s³ów -l liczenie lini


flnm=$(basename $flnm .zip)
flnm_txt="$flnm.txt"

number_lines=$(cat $flnm_txt |wc -l)	        		
to_mail_all_l="File has: $number_lines lines"
echo -e "\t\t$to_mail_all_l" >>PROCESSED/"$log_flnm"


sed -i '/||||||/d' "$flnm_txt" 						


Date_Stamp=$(date '+%Y%m%d%H%M%S')
echo "$Date_Stamp - Process of removing empty lines - Succes" >>PROCESSED/"$log_flnm"



file_wo_el=$(wc -l < "$flnm_txt")   			
empty_nlines="$(($number_lines-$file_wo_el))"
echo -e "\t\tFile has: $empty_nlines empty lines" >> PROCESSED/"$log_flnm"

# usuniêcie lini z niepoprawn¹ liczb¹ kolumn awk -v podanie parametru NF iloœæ wyst¹pien znaku


flnm_inc=${flnm}".bad"${TMSTP}

awk -v n="$3" -F'|' 'NF!=n ' "$flnm_txt" > "$flnm_inc"     
awk -v n="$3" -F'|' 'NF==n ' "$flnm_txt" > temp_ln.txt

mv temp_ln.txt "$flnm_txt" 

Date_Stamp=$(date '+%Y%m%d%H%M%S')
echo "$Date_Stamp - Removing rows with incorrect number of columns - Succes" >>PROCESSED/"$log_flnm"


incorrect_col=$(wc -l < "$flnm_inc")     							
echo -e "\t\tFile has: $incorrect_col rows with incorect number of column" >>PROCESSED/"$log_flnm"

# usuniêcie zduplikowanych wersów


awk 'NR == 1; NR>1 {print $0 |"sort -n"}' "$flnm_txt" > without_dup  
uniq -D without_dup  >> "$flnm_inc"					


Date_Stamp=$(date '+%Y%m%d%H%M%S')
echo "$Date_Stamp - Removing duplicated lines - Succes" >>PROCESSED/"$log_flnm"



uniq -u without_dup > "$flnm_txt"
rm without_dup

lines_with_dup=$(wc -l < "$flnm_inc")
duplicated_nlines="$(($lines_with_dup-$incorrect_col))"
to_mail_dup="File has: $duplicated_nlines duplicates"
echo -e "\t\t$to_mail_dup" >>PROCESSED/"$log_flnm"

# usuwanie wersów dla kolumny OrderQuantity wiekszej niz 100


awk -v val=$4 -F'|' '$5 >val || $5 ==""' "$flnm_txt" |tail -n +2 >> "$flnm_inc"  
head -n 1 "$flnm_txt" >header_line
cat header_line >temp_ord
awk -v val=$4 -F'|' '$5 <=val && $5!="" ' "$flnm_txt"  >>temp_ord


Date_Stamp=$(date '+%Y%m%d%H%M%S')
echo "$Date_Stamp - Removing  quantity bigger than $4 - Succes" >>PROCESSED/"$log_flnm"


mv temp_ord "$flnm_txt"

inc_lines_oq=$(wc -l < "$flnm_inc")
nr_lines_oq="$(($inc_lines_oq-$lines_with_dup))"
echo -e "\t\File has: $nr_lines_oq rows with OrderQuality bigger than $4" >>PROCESSED/"$log_flnm"


# Sprawdzenie ze starym plikiem

tail -n +2 "$flnm_txt" | sort > srt_new					
dos2unix -q "$5"   								
tail -n +2 "$5" | sort > srt_old

diff  srt_old srt_new  --changed-group-format=""  >> "$flnm_inc"


Date_Stamp=$(date '+%Y%m%d%H%M%S')
echo "$Date_Stamp -Removing lines which are avaiable in old file - Succes" >>PROCESSED/"$log_flnm"


all_inc_ln=$(wc -l <"$flnm_inc")
same_line=$(($all_inc_ln-$inc_lines_oq))
echo -e "\t\tFile has $same_line common lines with  our file" >> PROCESSED/"$log_flnm"

cat header_line > tmp_after_check 
diff srt_old srt_new --old-group-format=""  --unchanged-group-format=""  >>tmp_after_check
mv tmp_after_check "$flnm_txt"

# usuwanie linii zawieraj¹cych dane w kolumnie SecretCode

awk  -F'|' ' $7 !=""'  InternetSales_new.txt  | tail -n +2 | cut -d'|' -f -6 | awk '{print $0"|"}' >> "$flnm_inc"

Date_Stamp=$(date '+%Y%m%d%H%M%S')
echo "$Date_Stamp - Removing lines which have anything in SecretCode column - Succes" >>PROCESSED/"$log_flnm"


all_inc_ln_without_securecode=$(wc -l <"$flnm_inc")
lines_with_securecode=$(($all_inc_ln_without_securecode-$all_inc_ln))
echo -e "\t\tFile has $lines_with_securecode rows where SecretCode contains data" >>PROCESSED/"$log_flnm"

cat header_line >tmp_non_securecode
tail -n +2 InternetSales_new.txt|awk  -F'|' ' $7 ==""'  >>tmp_non_securecode 			
mv tmp_non_securecode "$flnm_txt"

rm srt_old srt_new

# Walidacja kolumny z imieniem i nazwiskiem


awk -F"|" '!match($3,",") ' "$flnm_txt" | tail -n +2  >> "$flnm_inc"

cat header_line >temp_name_sur
awk -F"|" 'match($3,",") ' "$flnm_txt"  >> temp_name_sur



Date_Stamp=$(date '+%Y%m%d%H%M%S')
echo "$Date_Stamp - Removing rows where name and surname do not have comma - Succes" >>PROCESSED/"$log_flnm"


mv temp_name_sur "$flnm_txt"
all_inc_ln_name_surname=$(wc -l <"$flnm_inc")
inc_lin_sur=$(($all_inc_ln_name_surname-$all_inc_ln_without_securecode))
to_mail_inc_lines="File has got $all_inc_ln_name_surname wrong or not appropriate lines"
echo -e "\t\tDowloaded file has $inc_lin_sur lines without comma between name and surname" >>PROCESSED/"$log_flnm"

#Podzia³ kolumny Customer_Name na FIRST_NAEM and LAST_NAME
 
echo "FIRST_NAME" > first_name
echo "LAST_NAME" > last_name
cut -d'|' -f-2 "$flnm_txt" >first2col
cut -d'|' -f4- "$flnm_txt"  >last4col
cut -d'|' -f3 "$flnm_txt" | tr -d "\""| cut -d','  -f2 |tail -n +2 >>first_name
cut -d'|' -f3 "$flnm_txt" | tr -d "\""| cut -d','  -f1 |tail -n +2 >> last_name 
paste -d'|' first2col first_name last_name last4col > "$flnm_txt"



Date_Stamp=$(date '+%Y%m%d%H%M%S')
echo "$Date_Stamp - Splitting column Customer_name to first and last name  - Succes" >>PROCESSED/"$log_flnm"


rm first2col last4col first_name last_name header_line

# Stworzenie tabeli w bazie mysql oraz dodanie danych


col1=$(head -n1 "$flnm_txt" |cut -d'|' -f1)
col2=$(head -n1 "$flnm_txt" |cut -d'|' -f2)
col3=$(head -n1 "$flnm_txt" |cut -d'|' -f3)
col4=$(head -n1 "$flnm_txt" |cut -d'|' -f4)
col5=$(head -n1 "$flnm_txt" |cut -d'|' -f5)
col6=$(head -n1 "$flnm_txt" |cut -d'|' -f6)
col7=$(head -n1 "$flnm_txt" |cut -d'|' -f7)
col8=$(head -n1 "$flnm_txt" |cut -d'|' -f8)

password_db=$(echo "$8" | base64 -d)
export MYSQL_PWD=$password_db 									
db_name="CUSTOMERS_$6"

mysql -u "$7" -h  "$9" -P 3306 -D "$7" --silent -e "CREATE TABLE $db_name($col1 INTEGER,$col2 VARCHAR(20),$col3 VARCHAR(40),$col4 VARCHAR(40),$col5 VARCHAR(20),$col6 VARCHAR(20),$col7 FLOAT,$col8 VARCHAR(20) );"


Date_Stamp=$(date '+%Y%m%d%H%M%S')
echo "$Date_Stamp - Creating table in db - Succes" >>PROCESSED/"$log_flnm"


tail -n +2 "$flnm_txt" | tr ',' '.' >flnm_txt_without_header   
mv "$flnm_txt" PROCESSED/

mysql -u "$7" -h  "$9" -P 3306 -D "$7" --silent -e "LOAD DATA LOCAL INFILE 'flnm_txt_without_header' INTO TABLE $db_name FIELDS TERMINATED BY '|';"


Date_Stamp=$(date '+%Y%m%d%H%M%S')
echo "$Date_Stamp - Inserting data to db table - Succes" >>PROCESSED/"$log_flnm"


rm flnm_txt_without_header


# aktualizacja wartosci dla kolumny SecretCode poprzez loswego stringa


random_string="$(openssl rand -hex 5)"

mysql -u "$7" -h  "$9" -P 3306 -D "$7" --silent -e "UPDATE $db_name SET $col8='$random_string';"


Date_Stamp=$(date '+%Y%m%d%H%M%S')
echo "$Date_Stamp - Updating table  - Succes" >>PROCESSED/"$log_flnm"

# wyeksportowanie danych z tabeli do csv


mysql -u "$7" -h  "$9" -P 3306 -D "$7" --silent -e "SELECT * FROM $db_name;"|sed 's/\t/,/g' > $db_name.csv


Date_Stamp=$(date '+%Y%m%d%H%M%S')
echo "$Date_Stamp - Export table to .csv file  - Succes" >>PROCESSED/"$log_flnm"


# Spakowanie do zipa .csv file


zip  -q $db_name $db_name.csv  

Date_Stamp=$(date '+%Y%m%d%H%M%S')
echo "$Date_Stamp - Zipping .csv file - Succes" >>PROCESSED/"$log_flnm"



to_mail_inc_lines="File has $all_inc_ln_name_surname incorrect lines"

good_lines=$(($number_lines-$all_inc_ln_name_surname))
mail_all_good="File has $good_lines correct lines"

count_table=$(mysql -u "$7" -h  "$9" -P 3306 -D "$7" --silent -e "SELECT COUNT(*) FROM $db_name;")
mail_insert="To database was loaded $count_table lines"

#echo -e "$to_mail_all_l\n$mail_all_good \n$to_mail_dup \n$to_mail_inc_lines \n$mail_insert"

# send emails with attachment

#echo -e "$to_mail_all_l\n$mail_all_good \n$to_mail_dup \n$to_mail_inc_lines \n$mail_insert" |mailx -s "CUSTOMERS LOAD - $TMSTP" ${10}
#mailx -s "$TMSTP,$good_lines "-a $db_name.zip -a $log_filename ${10}

Date_Stamp=$(date '+%Y%m%d%H%M%S')
echo "$Date_Stamp - Sending mail - Succes" >>PROCESSED/"$log_flnm"



