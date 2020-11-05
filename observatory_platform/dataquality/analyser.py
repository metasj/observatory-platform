# Copyright 2020 Curtin University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: Tuan Chien


from abc import ABC, abstractmethod

class DataQualityAnalyser(ABC):
    """ Data Quality Analyser Interface """

    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'run') and callable(subclass.run)
                or NotImplemented)

    @abstractmethod
    def run(self, **kwargs):
        """
        Run the analyser.
        @param kwargs: Optional key value arguments to pass into an analyser. See individual analyser documentation.
        """
        raise NotImplementedError
        pass

