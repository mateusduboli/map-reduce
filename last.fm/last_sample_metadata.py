#!/usr/bin/env python
# vim:ts=4:sts=4:sw=4:et:wrap:ai:fileencoding=utf-8:


__author__ = "Tiago Alves Macambira < first . last @ chaordicsystems.com>"
__copyright__ = "Copyright (C) 2013 Chaordic Systems S/A"
__license__ = "Public Domain"


from mrjob.job import MRJob
import mrjob.protocol

class LastfmSampleMetadata(MRJob):

    INPUT_PROTOCOL = mrjob.protocol.JSONProtocol

    def steps(self):
        return [
            self.mr(mapper=None,
                    reducer=self.reducer_group_data),
                   ]

    def reducer_group_data(self, key, values):
        temp_dict = dict()
        for d in values:
            temp_dict.update(d)
        yield (key, temp_dict)


if __name__ == '__main__':
    LastfmSampleMetadata.run()
