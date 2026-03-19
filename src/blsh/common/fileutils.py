import os


def create_file(dir, fname, contents):
    if not os.path.exists(dir):
        os.makedirs(dir)

    # 파일 생성 (쓰기 모드 'w')
    file_path = os.path.join(dir, fname)
    with open(file_path, "w") as f:
        f.write(contents)
