export LANG=ru_RU.UTF-8
count=$(wc -l < access.log) #считаем количество строк

uniq_ip=$(awk '{print $1}' access.log | sort | uniq -c | wc -l) #считаем количество уникальных ip

count_method() {
    awk '{print $6}' access.log | grep "$1" | wc -l # Функция для подсчета количества методов
}
count_grep=$(count_method "GET")
count_post=$(count_method "POST")

max_url=$(awk '{urls[$7]++} END {max=0; max_url=""; for (url in urls) {if (urls[url] > max) {max = urls[url]; max_url = url}} print max, max_url}' access.log) #считаем максимальный url

cat <<EOL > report.txt
Отчет о логе веб сервера

Общее количество запросов $count
Количество уникальных IP адресов $uniq_ip

Количество запросов по методам:
  $count_grep GET
  $count_post POST

Самый популярный URL: $max_url
EOL

 