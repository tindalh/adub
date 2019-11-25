import unittest
import sys
import os
sys.path.append("..")
import serviceTasks.importWatcher as wtchr

class TestImportWatcher(unittest.TestCase):
    # python -m unittest test_importWatcher.TestImportWatcher.test_startDirectoryWatch
    def test_startDirectoryWatch(self):
        """
            Once this test has started, modify a file in the file_path pass to startDirectoryWatch
        """
        def testFunc(str):
            print(f"Called from testImportWatcher with {str}")

        # function returns None so test for False
        wtchr.__startDirectoryWatch__("test", testFunc, "data/a_file_to_watch.csv")

        new_path= "data\\a_file_to_watch.csv" + "_"
        os.rename("data\\a_file_to_watch.csv",new_path)
        os.rename(new_path,"data\\a_file_to_watch.csv")

if __name__ == '__main__':
    unittest.main()