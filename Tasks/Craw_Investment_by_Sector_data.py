import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

options = Options()
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--window-size=1920,1080")

driver = webdriver.Chrome(options=options)

try:
    # Truy cập thẳng link gốc của PX-Web để bỏ qua iframe phức tạp
    direct_url = "https://pxweb.nso.gov.vn/pxweb/vi/%C4%90%E1%BA%A7u%20t%C6%B0/%C4%90%E1%BA%A7u%20t%C6%B0/V04.03.px/?rxid=dca48991-ae78-46df-97a5-26f8fbd2d847"
    driver.get(direct_url)
    wait = WebDriverWait(driver, 5)
    print('đã truy cập trang web')
    # Chọn dữ liệu
    boxes = wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "select-box")))
    for box in boxes:
        options_list = box.find_elements(By.TAG_NAME, "option")
        for opt in options_list:
            if not opt.is_selected():
                opt.click()

    # Nhấn Tiếp tục
    driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_btnContinue").click()

    # Chờ và chọn định dạng file (ví dụ Excel)
    fmt = wait.until(EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_pxControl_dropFileType")))
    fmt.send_keys("Excel (xlsx)")

    # Nhấn Tải về
    driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_pxControl_btnDownload").click()
    
    print("Yêu cầu tải về đã được gửi!")
    time.sleep(5) # Đợi tải file trong môi trường headless

finally:
    driver.quit()