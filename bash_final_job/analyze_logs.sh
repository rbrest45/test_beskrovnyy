export LANG=ru_RU.UTF-8
count=$(wc -l < access.log) #������� ���������� �����

uniq_ip=$(awk '{print $1}' access.log | sort | uniq -c | wc -l) #������� ���������� ���������� ip

count_method() {
    awk '{print $6}' access.log | grep "$1" | wc -l # ������� ��� �������� ���������� �������
}
count_grep=$(count_method "GET")
count_post=$(count_method "POST")

max_url=$(awk '{urls[$7]++} END {max=0; max_url=""; for (url in urls) {if (urls[url] > max) {max = urls[url]; max_url = url}} print max, max_url}' access.log) #������� ������������ url

cat <<EOL > report.txt
����� � ���� ��� �������

����� ���������� �������� $count
���������� ���������� IP ������� $uniq_ip

���������� �������� �� �������:
  $count_grep GET
  $count_post POST

����� ���������� URL: $max_url
EOL

 