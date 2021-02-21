# VN-text-datasets

Vietnamese Text Datasets

Forums:

- https://voz.vn/
- https://tinhte.vn/forums/
- https://www.otofun.net/forums/
- https://vn-z.vn/forums/
- http://f319.com/
- https://diendan.hocmai.vn/

## Dependencies

- aiohttp
- Beautiful Soup 4

Using `conda`

```python
# (recommended) create virtual environment
conda create -n env python=3.8
conda activate env

conda install aiohttp beautifulsoup4
conda install -c conda-forge cchardet, aiodns, brotlipy
```

Using `pip`

```python
python -m venv env
./env/Scripts/activate

pip install aiohttp[speedups] beautifulsoup4
```
