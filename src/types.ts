export type MovieQueryParams = {
    page?: number;
    pageSize?: number;
    genre?: string;
    minRating?: number;
    year?: number;
    sortBy?: string;
    search?: string;
};

export type MovieImportResult = {
    success: boolean;
    count: number;
    errors?: string[];
};
