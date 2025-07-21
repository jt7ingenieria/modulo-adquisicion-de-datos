import subprocess
import os

def activate_venv():
    venv_path = r'C:\Users\javie\modulos bot\entorno_virtual_modulo\Scripts\Activate.ps1'
    command = f'powershell.exe -NoExit -Command "& \'{venv_path}\'"'
    while True:
        process = subprocess.Popen(command, shell=True)
        process.wait()  # Espera a que el proceso termine (terminal se cierre)
        print("Terminal cerrado. Reactivando...")

if __name__ == '__main__':
    activate_venv()