"""
Trade Processing Pipeline Launcher
Simple GUI launcher for the Streamlit application
"""

import os
import sys
import subprocess
import webbrowser
import time
import threading
import tkinter as tk
from tkinter import messagebox, scrolledtext
import psutil

class StreamlitLauncher:
    def __init__(self):
        self.process = None
        self.root = tk.Tk()
        self.root.title("Trade Processing Pipeline Launcher")
        self.root.geometry("600x400")
        self.root.resizable(False, False)

        # Configure style
        self.root.configure(bg="#f0f0f0")

        # Create UI elements
        self.create_ui()

        # Check dependencies on startup
        self.check_dependencies()

    def create_ui(self):
        # Title
        title_frame = tk.Frame(self.root, bg="#1f77b4", height=60)
        title_frame.pack(fill=tk.X)

        title_label = tk.Label(
            title_frame,
            text="Trade Processing Pipeline",
            font=("Arial", 18, "bold"),
            bg="#1f77b4",
            fg="white"
        )
        title_label.pack(pady=15)

        # Status frame
        status_frame = tk.Frame(self.root, bg="#f0f0f0")
        status_frame.pack(fill=tk.X, padx=20, pady=10)

        self.status_label = tk.Label(
            status_frame,
            text="Status: Ready to launch",
            font=("Arial", 10),
            bg="#f0f0f0"
        )
        self.status_label.pack(anchor=tk.W)

        # Button frame
        button_frame = tk.Frame(self.root, bg="#f0f0f0")
        button_frame.pack(pady=20)

        self.launch_button = tk.Button(
            button_frame,
            text="🚀 Launch Application",
            command=self.launch_app,
            font=("Arial", 12, "bold"),
            bg="#4CAF50",
            fg="white",
            width=20,
            height=2,
            cursor="hand2"
        )
        self.launch_button.pack(side=tk.LEFT, padx=10)

        self.stop_button = tk.Button(
            button_frame,
            text="⏹ Stop Application",
            command=self.stop_app,
            font=("Arial", 12, "bold"),
            bg="#f44336",
            fg="white",
            width=20,
            height=2,
            cursor="hand2",
            state=tk.DISABLED
        )
        self.stop_button.pack(side=tk.LEFT, padx=10)

        # Output frame
        output_frame = tk.Frame(self.root, bg="#f0f0f0")
        output_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=10)

        output_label = tk.Label(
            output_frame,
            text="Application Output:",
            font=("Arial", 10, "bold"),
            bg="#f0f0f0"
        )
        output_label.pack(anchor=tk.W)

        self.output_text = scrolledtext.ScrolledText(
            output_frame,
            height=10,
            width=70,
            font=("Consolas", 9),
            bg="#1e1e1e",
            fg="#00ff00"
        )
        self.output_text.pack(fill=tk.BOTH, expand=True)

        # Footer
        footer_label = tk.Label(
            self.root,
            text="The app will open in your browser at http://localhost:8501",
            font=("Arial", 9),
            bg="#f0f0f0",
            fg="#666"
        )
        footer_label.pack(pady=5)

    def check_dependencies(self):
        """Check if all required dependencies are installed"""
        try:
            import streamlit
            import pandas
            import openpyxl
            self.log_output("✓ All dependencies verified\n")
        except ImportError as e:
            self.log_output(f"❌ Missing dependency: {e}\n")
            self.log_output("Please run: pip install -r requirements.txt\n")
            self.launch_button.config(state=tk.DISABLED)

    def log_output(self, text):
        """Add text to output window"""
        self.output_text.insert(tk.END, text)
        self.output_text.see(tk.END)
        self.root.update()

    def launch_app(self):
        """Launch the Streamlit application"""
        if self.process:
            messagebox.showwarning("Warning", "Application is already running!")
            return

        try:
            self.status_label.config(text="Status: Launching application...")
            self.log_output("Starting Streamlit server...\n")

            # Start Streamlit in a subprocess
            cmd = [sys.executable, "-m", "streamlit", "run", "unified-streamlit-app.py"]
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True
            )

            # Start thread to read output
            output_thread = threading.Thread(target=self.read_output, daemon=True)
            output_thread.start()

            # Update UI
            self.launch_button.config(state=tk.DISABLED)
            self.stop_button.config(state=tk.NORMAL)
            self.status_label.config(text="Status: Application running", fg="green")

            # Wait a bit then open browser
            time.sleep(3)
            webbrowser.open("http://localhost:8501")

        except Exception as e:
            messagebox.showerror("Error", f"Failed to launch application:\n{e}")
            self.status_label.config(text="Status: Launch failed", fg="red")
            self.log_output(f"Error: {e}\n")

    def read_output(self):
        """Read and display output from Streamlit process"""
        try:
            for line in iter(self.process.stdout.readline, ''):
                if line:
                    self.output_text.insert(tk.END, line)
                    self.output_text.see(tk.END)
                    self.root.update()
        except:
            pass

    def stop_app(self):
        """Stop the Streamlit application"""
        if not self.process:
            return

        try:
            self.log_output("\nStopping application...\n")

            # Kill the process and its children
            parent = psutil.Process(self.process.pid)
            children = parent.children(recursive=True)

            for child in children:
                child.kill()
            parent.kill()

            self.process = None

            # Update UI
            self.launch_button.config(state=tk.NORMAL)
            self.stop_button.config(state=tk.DISABLED)
            self.status_label.config(text="Status: Application stopped", fg="red")
            self.log_output("Application stopped successfully\n")

        except Exception as e:
            messagebox.showerror("Error", f"Failed to stop application:\n{e}")
            self.log_output(f"Error stopping: {e}\n")

    def on_closing(self):
        """Handle window closing"""
        if self.process:
            if messagebox.askokcancel("Quit", "Stop the application and exit?"):
                self.stop_app()
                self.root.destroy()
        else:
            self.root.destroy()

    def run(self):
        """Run the launcher"""
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        self.root.mainloop()

if __name__ == "__main__":
    launcher = StreamlitLauncher()
    launcher.run()