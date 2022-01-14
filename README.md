# arrow_flight

動作方法

Flight Server

$ python3 server.py


パケットキャプチャプログラム

$ ./target/debug/arrows <インタフェースネーム>


ファイル出力などを行うclient

$ python3 client.py

✴︎ clientは現在サーバにある全てのデータを書き出すようになっている
　変更するには、client.pyのプログラムの198行目と199行目のコメント
　アウトを外し（文字列の先頭にある#を消す）、200行目をコメントア
　ウトを行うことで現在時間から１分前までのデータを書き出すように
　変更を行うことができる。

python3を動作させるために

Server.pyとclient.pyではpandasやpyarrowを使用しているため、これらの
インストールが終了していないと動作することができないと思われる。
そのため、インストールが終了していない場合にはインストールをすることを
推奨する。

$ pip install pyarrow
$ pip install pandas
