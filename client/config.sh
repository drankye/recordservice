# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Source this file from the $RECORD_SERVICE_HOME directory to
# setup your environment. If $RECORD_SERVICE_HOME is undefined
# this script will set it to the current working directory.

if [ -z $RECORD_SERVICE_HOME ]; then
  export RECORD_SERVICE_HOME=$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)
fi
