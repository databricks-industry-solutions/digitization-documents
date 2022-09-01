# tika-ocr-inputformat


```python
spark.read.format('tika').load(path_to_any_file)
```

|                path|length|    modificationTime|             content|         contentType|         contentText|     contentMetadata|
| ------------------ | ---- | ------------------ | ------------------ | ------------------ | ------------------ | ------------------ |
|file:/Users/antoi...| 36864|2022-08-25 14:15:...|[D0 CF 11 E0 A1 B...|  application/msword|key\n\nvalue\n\nh...|{meta:page-count ...|
|file:/Users/antoi...| 34030|2022-08-25 14:16:...|[89 50 4E 47 0D 0...|           image/png|key\n\nvalue\n\nh...|{tiff:BitsPerSamp...|
|file:/Users/antoi...| 26294|2022-08-25 14:13:...|[50 4B 03 04 14 0...|application/vnd.o...|\n\n\nimage1.png\...|{meta:page-count ...|
|file:/Users/antoi...| 22805|2022-08-25 14:13:...|[25 50 44 46 2D 3...|     application/pdf|\n \n \n\n \n\nke...|{dc:format -> app...|


