export type Key = Key[] | string | symbol | number | boolean | Uint8Array;

export type BufferWithDataView = Buffer & {
	dataView?: DataView;
	start?: number;
	end?: number;
};
